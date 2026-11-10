/*
Copyright The Alpheus Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

//go:embed ui
var uiFiles embed.FS

// maxErrorEntries is how many entries of a replica's error log the page
// shows.
const maxErrorEntries = 1000

func uiCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "ui", "", &o)
	listen := fs.String("listen", "127.0.0.1:7171", "where the page is served; keep it on this machine unless --in-cluster")
	noOpen := fs.Bool("no-open", false, "do not open the page in the browser")
	inCluster := fs.Bool("in-cluster", false, "run as a Deployment in the cluster: runs of every namespace, replays as pods")
	var hosts stringList
	fs.Var(&hosts, "host", "a host name the page may be reached by besides localhost, such as an Ingress's; can be repeated")
	agentImage := fs.String("agent-image", "", "with --in-cluster: the agent's image, for replay pods of runs without status.agentImage")
	replayMode := fs.String("replay", "", "how the replay button runs: podman, docker, cluster or off; default podman, or cluster with --in-cluster")
	caFile := fs.String("ca-file", "", "CA bundle for the storage endpoint")
	endpoint := fs.String("endpoint", "", "the S3 endpoint as seen from here, when spec.storage.endpoint is only reachable in the cluster")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return usagef("ui takes no arguments")
	}
	if *replayMode == "" {
		*replayMode = "podman"
		if *inCluster {
			*replayMode = "cluster"
		}
	}
	if !slices.Contains([]string{"podman", "docker", "cluster", "off"}, *replayMode) {
		return usagef("--replay %s: want podman, docker, cluster or off", *replayMode)
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	if *inCluster {
		*noOpen = true
		if o.kube.Namespace == "" {
			ns = allNamespaces // the Deployment's own namespace has no runs
		}
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	u := &ui{e: e, c: c, namespace: ns, caFile: *caFile, endpoint: *endpoint, token: hex.EncodeToString(token), addr: ln.Addr().String(),
		hosts: hosts, replayMode: *replayMode, agentImage: *agentImage}
	srv := &http.Server{Handler: u.handler(), ReadHeaderTimeout: 10 * time.Second}
	url := "http://" + u.addr + "/"
	if *inCluster {
		url = "port " + u.addr
	}
	fmt.Fprintf(e.Stdout, "alpheus ui on %s (namespace %s); Ctrl-C stops it\n", url, ns)
	if !*noOpen {
		openBrowser(url)
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func openBrowser(url string) {
	cmd := "xdg-open"
	if runtime.GOOS == "darwin" {
		cmd = "open"
	}
	_ = exec.Command(cmd, url).Start()
}

// ui serves the page and a read-only JSON API over the cluster and the
// runs' storage, with the user's kubeconfig (GO-142).
type ui struct {
	e         *Env
	c         *Clients
	namespace string
	caFile    string
	endpoint  string
	token     string // guards replay, which runs a program or starts a pod
	addr      string
	// hosts are the host names besides localhost the page answers to.
	hosts []string
	// replayMode is how the replay button runs: podman, docker, cluster
	// or off.
	replayMode string
	agentImage string // the agent's image, for replay pods

	mu     sync.Mutex
	stores map[string]store.Store // by run UID
}

func (u *ui) handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(uiFiles, "ui")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /{$}", u.index)
	mux.HandleFunc("GET /api/runs", u.runs)
	mux.HandleFunc("GET /api/watch", u.watch)
	mux.HandleFunc("GET /api/runs/{ns}/{name}", u.run)
	mux.HandleFunc("GET /api/runs/{ns}/{name}/attempts", u.attempts)
	mux.HandleFunc("GET /api/runs/{ns}/{name}/logs", u.logs)
	mux.HandleFunc("GET /api/runs/{ns}/{name}/errors", u.errorLog)
	mux.HandleFunc("GET /api/runs/{ns}/{name}/failure", u.failure)
	mux.HandleFunc("POST /api/runs/{ns}/{name}/replay", u.replay)
	return u.local(mux)
}

// local refuses requests addressed to another host, so a web page cannot
// reach the cluster through this server by DNS rebinding, and requests
// that change something from another origin.
func (u *ui) local(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) && !slices.Contains(u.hosts, host) {
			http.Error(w, "alpheus ui answers only on localhost", http.StatusForbidden)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (u *ui) index(w http.ResponseWriter, r *http.Request) {
	page, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	page = bytes.Replace(page, []byte("{{namespace}}"), []byte(u.namespace), 1)
	page = bytes.Replace(page, []byte("{{token}}"), []byte(u.token), 1)
	page = bytes.Replace(page, []byte("{{replay}}"), []byte(u.replayMode), 1)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page) // a browser that went away needs no answer
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v) // a browser that went away needs no answer
}

func httpError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if isNotFound(err) || errors.Is(err, store.ErrNotFound) {
		code = http.StatusNotFound
	}
	http.Error(w, err.Error(), code)
}

// allNamespaces, as the namespace, lists the runs of every namespace.
const allNamespaces = "*"

// listNamespace is the namespace a list or a watch is about; "" is every
// namespace.
func (u *ui) listNamespace(r *http.Request) string {
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = u.namespace
	}
	if ns == allNamespaces {
		return ""
	}
	return ns
}

// runSummary is one run in the list.
type runSummary struct {
	Namespace     string            `json:"namespace"`
	Name          string            `json:"name"`
	Phase         v1alpha1.RunPhase `json:"phase"`
	Reason        v1alpha1.Reason   `json:"reason,omitempty"`
	Message       string            `json:"message,omitempty"`
	StepsFinished string            `json:"stepsFinished,omitempty"`
	Created       time.Time         `json:"created"`
	StartedAt     *metav1.Time      `json:"startedAt,omitempty"`
	FinishedAt    *metav1.Time      `json:"finishedAt,omitempty"`
}

func (u *ui) runs(w http.ResponseWriter, r *http.Request) {
	var list v1alpha1.StreamRunList
	if err := u.c.Client.List(r.Context(), &list, client.InNamespace(u.listNamespace(r))); err != nil {
		httpError(w, err)
		return
	}
	out := []runSummary{}
	for _, run := range list.Items {
		out = append(out, runSummary{Namespace: run.Namespace, Name: run.Name, Phase: run.Status.Phase, Reason: run.Status.Reason,
			Message: run.Status.Message, StepsFinished: run.Status.StepsFinished, Created: run.CreationTimestamp.Time,
			StartedAt: run.Status.StartedAt, FinishedAt: run.Status.FinishedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })
	writeJSON(w, out)
}

// watch sends a server-sent event whenever a run in the namespace changes,
// so the page fetches it again.
func (u *ui) watch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	wi, err := u.c.Client.Watch(r.Context(), &v1alpha1.StreamRunList{}, client.InNamespace(u.listNamespace(r)))
	if err != nil {
		httpError(w, err)
		return
	}
	defer wi.Stop()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Fprint(w, ": watching\n\n")
	flusher.Flush()
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return
			}
			run, isRun := ev.Object.(*v1alpha1.StreamRun)
			if !isRun || ev.Type == watch.Bookmark {
				continue
			}
			data, _ := json.Marshal(map[string]string{"type": string(ev.Type), "namespace": run.Namespace, "name": run.Name})
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// runView is one run with its events.
type runView struct {
	Run    *v1alpha1.StreamRun `json:"run"`
	Events []eventView         `json:"events"`
}

type eventView struct {
	Time   time.Time `json:"time"`
	Type   string    `json:"type"`
	Reason string    `json:"reason"`
	Action string    `json:"action"`
	Note   string    `json:"note"`
}

func (u *ui) get(r *http.Request) (*v1alpha1.StreamRun, error) {
	var run v1alpha1.StreamRun
	err := u.c.Client.Get(r.Context(), client.ObjectKey{Namespace: r.PathValue("ns"), Name: r.PathValue("name")}, &run)
	return &run, err
}

func (u *ui) run(w http.ResponseWriter, r *http.Request) {
	run, err := u.get(r)
	if err != nil {
		httpError(w, err)
		return
	}
	view := runView{Run: run, Events: []eventView{}}
	list, err := u.c.Kube.EventsV1().Events(run.Namespace).List(r.Context(), metav1.ListOptions{
		FieldSelector: "regarding.kind=StreamRun,regarding.name=" + run.Name,
	})
	if err == nil {
		for _, ev := range list.Items {
			if ev.Regarding.UID != "" && ev.Regarding.UID != run.UID {
				continue // an earlier run of the same name
			}
			view.Events = append(view.Events, eventView{Time: eventTime(&ev), Type: ev.Type, Reason: ev.Reason, Action: ev.Action, Note: ev.Note})
		}
		sort.Slice(view.Events, func(i, j int) bool { return view.Events[i].Time.Before(view.Events[j].Time) })
	}
	writeJSON(w, view)
}

func eventTime(ev *eventsv1.Event) time.Time {
	switch {
	case !ev.EventTime.IsZero():
		return ev.EventTime.Time
	case ev.Series != nil:
		return ev.Series.LastObservedTime.Time
	}
	return ev.DeprecatedLastTimestamp.Time
}

// storeOf opens a run's storage once and keeps it.
func (u *ui) storeOf(ctx context.Context, run *v1alpha1.StreamRun) (store.Store, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if st, ok := u.stores[string(run.UID)]; ok {
		return st, nil
	}
	st, err := u.e.OpenStore(ctx, u.c.Client, run, store.Address{Endpoint: u.endpoint, CAFile: u.caFile})
	if err != nil {
		return nil, err
	}
	if u.stores == nil {
		u.stores = map[string]store.Store{}
	}
	u.stores[string(run.UID)] = st
	return st, nil
}

// target is the run, step and replica a request is about.
func (u *ui) target(w http.ResponseWriter, r *http.Request) (*v1alpha1.StreamRun, string, int, store.Store, bool) {
	run, err := u.get(r)
	if err != nil {
		httpError(w, err)
		return nil, "", 0, nil, false
	}
	step := r.URL.Query().Get("step")
	replica, err := strconv.Atoi(r.URL.Query().Get("replica"))
	if step == "" || err != nil || replica < 0 {
		http.Error(w, "step and replica are required", http.StatusBadRequest)
		return nil, "", 0, nil, false
	}
	st, err := u.storeOf(r.Context(), run)
	if err != nil {
		httpError(w, err)
		return nil, "", 0, nil, false
	}
	return run, step, replica, st, true
}

// attempts lists the attempts of a replica whose logs or failure are kept.
func (u *ui) attempts(w http.ResponseWriter, r *http.Request) {
	run, step, replica, st, ok := u.target(w, r)
	if !ok {
		return
	}
	type attemptView struct {
		Attempt int  `json:"attempt"`
		Logs    bool `json:"logs"`
		Failure bool `json:"failure"`
		// Skipped is the entry for the chunks the replica never processed
		// after it gave up with failurePolicy continue (GO-97), last.
		Skipped bool `json:"skipped,omitempty"`
	}
	found := map[int]*attemptView{}
	for _, kind := range []struct {
		prefix string
		set    func(*attemptView)
	}{
		{chunk.LogsPrefix(run.Name, string(run.UID), step, replica, 1), func(a *attemptView) { a.Logs = true }},
		{chunk.FailurePrefix(run.Name, string(run.UID), step, replica, 1), func(a *attemptView) { a.Failure = true }},
	} {
		base := kind.prefix[:strings.LastIndex(strings.TrimSuffix(kind.prefix, "/"), "/")+1]
		for key, err := range st.List(r.Context(), base) {
			if err != nil {
				httpError(w, err)
				return
			}
			rest, ok := strings.CutPrefix(key, base+"attempt-")
			if !ok {
				continue
			}
			n, _, _ := strings.Cut(rest, "/")
			a, err := strconv.Atoi(n)
			if err != nil {
				continue
			}
			if found[a] == nil {
				found[a] = &attemptView{Attempt: a}
			}
			kind.set(found[a])
		}
	}
	out := []attemptView{}
	for _, a := range found {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt > out[j].Attempt })
	key := chunk.SkippedPrefix(run.Name, string(run.UID), step, replica) + "failure.json"
	for k, err := range st.List(r.Context(), key) {
		if err == nil && k == key {
			out = append(out, attemptView{Failure: true, Skipped: true})
		}
	}
	writeJSON(w, out)
}

// logs is a kept log of a failed attempt, or with attempt 0 the live log
// of the replica's current pod.
func (u *ui) logs(w http.ResponseWriter, r *http.Request) {
	run, step, replica, st, ok := u.target(w, r)
	if !ok {
		return
	}
	container := r.URL.Query().Get("container")
	if container != controller.AgentContainer {
		container = controller.ComponentContainer
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	attempt, _ := strconv.Atoi(r.URL.Query().Get("attempt"))
	if attempt > 0 {
		body, err := st.Get(r.Context(), chunk.LogsPrefix(run.Name, string(run.UID), step, replica, attempt)+container+".log")
		if err != nil {
			httpError(w, err)
			return
		}
		defer body.Close()
		_, _ = io.Copy(w, body)
		return
	}
	pod := ""
	for _, ss := range run.Status.Steps {
		if ss.Name == step {
			for _, rs := range ss.Replicas {
				if int(rs.Index) == replica {
					pod = rs.Pod
				}
			}
		}
	}
	if pod == "" {
		http.Error(w, "the replica has no pod now; choose a kept attempt", http.StatusNotFound)
		return
	}
	tail := int64(2000)
	body, err := u.c.Kube.CoreV1().Pods(run.Namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container, TailLines: &tail}).Stream(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}
	defer body.Close()
	_, _ = io.Copy(w, body)
}

// errorLog is a replica's committed error log (GO-118), at most
// maxErrorEntries entries.
func (u *ui) errorLog(w http.ResponseWriter, r *http.Request) {
	run, step, replica, st, ok := u.target(w, r)
	if !ok {
		return
	}
	prefix := chunk.ErrorLogPrefix(run.Name, string(run.UID), step, replica)
	entries := []json.RawMessage{}
	for key, err := range st.List(r.Context(), prefix) {
		if err != nil {
			httpError(w, err)
			return
		}
		if !strings.HasSuffix(key, ".jsonl") || len(entries) >= maxErrorEntries {
			continue
		}
		data, err := store.ReadAll(r.Context(), st, key)
		if err != nil {
			httpError(w, err)
			return
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() && len(entries) < maxErrorEntries {
			if line := bytes.TrimSpace(sc.Bytes()); json.Valid(line) {
				entries = append(entries, append(json.RawMessage(nil), line...))
			}
		}
	}
	writeJSON(w, entries)
}

func (u *ui) failure(w http.ResponseWriter, r *http.Request) {
	run, step, replica, st, ok := u.target(w, r)
	if !ok {
		return
	}
	var prefix string
	if a := r.URL.Query().Get("attempt"); a == "skipped" {
		prefix = chunk.SkippedPrefix(run.Name, string(run.UID), step, replica)
	} else {
		attempt, _ := strconv.Atoi(a)
		var err error
		if prefix, err = replay.Find(r.Context(), st, run.Name, string(run.UID), step, replica, attempt); err != nil {
			httpError(w, err)
			return
		}
	}
	data, err := store.ReadAll(r.Context(), st, prefix+"failure.json")
	if err != nil {
		httpError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

// replay runs alpheus replay for a failed attempt and streams its output.
// It runs a program on this machine, so it needs the page's token.
func (u *ui) replay(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Alpheus-Token") != u.token {
		http.Error(w, "replay needs the page's token", http.StatusForbidden)
		return
	}
	if u.replayMode == "off" {
		http.Error(w, "replay is off here", http.StatusForbidden)
		return
	}
	step, replica, attempt := r.URL.Query().Get("step"), r.URL.Query().Get("replica"), r.URL.Query().Get("attempt")
	if _, err := strconv.Atoi(replica); err != nil || step == "" {
		http.Error(w, "step and replica are required", http.StatusBadRequest)
		return
	}
	args := []string{"replay", r.PathValue("name"), step, "--replica", replica, "-n", r.PathValue("ns")}
	switch _, err := strconv.Atoi(attempt); {
	case attempt == "skipped":
		args = append(args, "--skipped")
	case err == nil:
		args = append(args, "--attempt", attempt)
	case attempt != "":
		http.Error(w, "attempt is a number or skipped", http.StatusBadRequest)
		return
	}
	args = append(args, "--with", u.replayMode)
	if u.endpoint != "" {
		args = append(args, "--endpoint", u.endpoint)
	}
	if u.caFile != "" {
		args = append(args, "--ca-file", u.caFile)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	out := &flushWriter{w: w}
	fmt.Fprintf(out, "$ alpheus %s\n\n", strings.Join(args, " "))
	env := *u.e
	env.Stdout, env.Stderr = out, out
	env.Connect = func(Kube) (*Clients, error) { return u.c, nil }
	env.AgentImage = u.agentImage
	if code := Main(r.Context(), args, &env); code != 0 {
		fmt.Fprintf(out, "\nalpheus replay exited with code %d\n", code)
	}
}

// flushWriter sends each write to the browser at once.
type flushWriter struct {
	mu sync.Mutex
	w  http.ResponseWriter
}

func (f *flushWriter) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.w.Write(p)
	if fl, ok := f.w.(http.Flusher); ok {
		fl.Flush()
	}
	return n, err
}
