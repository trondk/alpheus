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

// Package cli is the alpheus command (SPEC 9). It talks to the cluster
// with the user's kubeconfig and has no server or login of its own
// (GO-43).
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// Env is what the commands run with. Tests replace the clients and the
// storage.
type Env struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Connect returns clients for the cluster the flags name.
	Connect func(k Kube) (*Clients, error)
	// OpenStore opens a run's storage with its storage Secret, at the
	// address the storage is reached by from here.
	OpenStore func(ctx context.Context, c client.Reader, r *v1alpha1.StreamRun, at store.Address) (store.Store, error)
	// NewS3 opens storage named by flags, for results gc.
	NewS3 func(store.S3Config) (store.Store, error)
	// RunCommand runs a program, such as podman for replay, and returns
	// its exit code.
	RunCommand func(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error)
	Now        func() time.Time
	// Getenv reads the environment; nil means os.Getenv.
	Getenv func(string) string
}

// Clients are the connections to one cluster.
type Clients struct {
	Client client.WithWatch
	Kube   kubernetes.Interface
	// Namespace is the kubeconfig context's namespace, or default.
	Namespace string
}

// DefaultEnv runs against the cluster in the user's kubeconfig and the real
// storage.
func DefaultEnv() *Env {
	return &Env{
		Stdin:      os.Stdin,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
		Connect:    connect,
		OpenStore:  controller.OpenStore,
		NewS3:      func(c store.S3Config) (store.Store, error) { return store.NewS3(c) },
		RunCommand: runCommand,
		Now:        time.Now,
	}
}

// Kube are the flags that pick the cluster and namespace.
type Kube struct {
	Kubeconfig string
	Context    string
	Namespace  string
}

func connect(k Kube) (*Clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = k.Kubeconfig
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: k.Context})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, hint(fmt.Errorf("kubeconfig: %w", err), "set KUBECONFIG, or pass --kubeconfig and --context")
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: Scheme()})
	if err != nil {
		return nil, hint(fmt.Errorf("connect to the cluster: %w", err), "check that the cluster in your kubeconfig is reachable")
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Clients{Client: c, Kube: kube, Namespace: ns}, nil
}

// Scheme has the types the CLI reads and writes.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// hintError is an error with what the user can do about it (GO-45).
type hintError struct {
	err  error
	hint string
}

func (e *hintError) Error() string { return e.err.Error() }
func (e *hintError) Unwrap() error { return e.err }

func hint(err error, h string) error { return &hintError{err: err, hint: h} }

// usageError is a wrong command line; it exits with 2.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usagef(format string, a ...any) error { return &usageError{fmt.Sprintf(format, a...)} }

type command struct {
	name    string
	args    string
	summary string
	run     func(ctx context.Context, e *Env, args []string) error
}

func commands() []command {
	return []command{
		{"run", "-f flow.yaml", "Create a StreamRun from a file", runCmd},
		{"get", "[run]", "Show runs, or one run's status", getCmd},
		{"watch", "<run>", "Follow a run's status live, with the lag of each edge", watchCmd},
		{"logs", "<run> [step]", "Show the logs of a run's containers", logsCmd},
		{"cancel", "<run>", "Cancel a run", cancelCmd},
		{"chunks", "<run> <edge>", "Show the manifest of an edge or a result", chunksCmd},
		{"replay", "<run> <step>", "Run a failed attempt again on its own, on its inputs, at debug level", replayCmd},
		{"ui", "", "Open a page in the browser that shows the runs live", uiCmd},
		{"validate", "-f flow.yaml", "Check a flow file without creating it", validateCmd},
		{"results", "get|cat|ls|copy|gc", "Fetch a run's results, write one to stdout, list them as S3 paths, copy them to another bucket, or delete expired ones", resultsCmd},
		{"errors", "<run> [step]", "Show the error log the components wrote", errorsCmd},
		{"version", "", "Show the CLI, controller and CRD versions", versionCmd},
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "alpheus runs and inspects StreamRuns on Kubernetes.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	for _, c := range commands() {
		fmt.Fprintf(w, "  alpheus %-8s %-16s %s\n", c.name, c.args, c.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Every command takes --kubeconfig, --context, -n/--namespace and -o json|yaml.")
	fmt.Fprintln(w, "Run alpheus <command> -h for its flags.")
}

// Main runs the command line args and returns the exit code.
func Main(ctx context.Context, args []string, e *Env) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		usage(e.Stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	for _, c := range commands() {
		if c.name != args[0] {
			continue
		}
		err := c.run(ctx, e, args[1:])
		var ue *usageError
		var he *hintError
		switch {
		case err == nil:
			return 0
		case errors.Is(err, flag.ErrHelp):
			return 0
		case errors.As(err, &ue):
			fmt.Fprintf(e.Stderr, "error: %s\nRun alpheus %s -h for help.\n", ue.msg, c.name)
			return 2
		case errors.As(err, &he):
			fmt.Fprintf(e.Stderr, "error: %s\n%s\n", err, he.hint)
		default:
			fmt.Fprintf(e.Stderr, "error: %s\n", err)
		}
		return 1
	}
	fmt.Fprintf(e.Stderr, "error: unknown command %q\n\n", args[0])
	usage(e.Stderr)
	return 2
}

// opts are the flags every command has.
type opts struct {
	kube   Kube
	output string
}

func newFlags(e *Env, name, args string, o *opts) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(e.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(e.Stderr, "Usage: %s [flags]\n\nFlags:\n", strings.Join(strings.Fields("alpheus "+name+" "+args), " "))
		fs.PrintDefaults()
	}
	fs.StringVar(&o.kube.Kubeconfig, "kubeconfig", "", "the kubeconfig file (default: KUBECONFIG or ~/.kube/config)")
	fs.StringVar(&o.kube.Context, "context", "", "the kubeconfig context (default: the current one)")
	fs.StringVar(&o.kube.Namespace, "namespace", "", "the namespace (default: the context's)")
	fs.StringVar(&o.kube.Namespace, "n", "", "short for --namespace")
	fs.StringVar(&o.output, "output", "", "json or yaml, for scripts (default: text)")
	fs.StringVar(&o.output, "o", "", "short for --output")
	return fs
}

// parse parses flags before, between and after the positional arguments,
// and returns those.
func parse(fs *flag.FlagSet, args []string, o *opts) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, err
			}
			return nil, &usageError{err.Error()}
		}
		args = fs.Args()
		if len(args) == 0 {
			break
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
	switch o.output {
	case "", "json", "yaml":
	default:
		return nil, usagef("-o %s: want json or yaml", o.output)
	}
	return pos, nil
}

// connect connects with the flags and returns the namespace to use.
func (e *Env) connect(o *opts) (*Clients, string, error) {
	c, err := e.Connect(o.kube)
	if err != nil {
		return nil, "", err
	}
	ns := o.kube.Namespace
	if ns == "" {
		ns = c.Namespace
	}
	if ns == "" {
		ns = "default"
	}
	return c, ns, nil
}

// notFound turns a missing run into an error that says how to find runs.
func notFound(err error, ns, name string) error {
	if isNotFound(err) {
		return hint(fmt.Errorf("streamrun %q not found in namespace %s", name, ns), "list the runs with: alpheus get -n "+ns)
	}
	return apiError(err)
}

// apiError explains the errors from the cluster a user can act on.
func apiError(err error) error {
	switch {
	case isNoMatch(err):
		return hint(fmt.Errorf("the cluster has no StreamRun resource: %w", err),
			"install Alpheus first: helm install alpheus oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus -n alpheus-system --create-namespace")
	case isForbidden(err):
		return hint(err, "ask for RBAC access to streamruns.alpheus-dev.org in this namespace")
	case strings.Contains(err.Error(), "connection refused"), strings.Contains(err.Error(), "no such host"):
		return hint(err, "check that the cluster in your kubeconfig is reachable, or pick another with --context")
	}
	return err
}
