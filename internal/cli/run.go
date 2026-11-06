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
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

// readFlow reads a StreamRun from a flow file, or from stdin for "-".
// Unknown fields are an error, so a misspelt field is not silently lost.
func readFlow(e *Env, file string) (*v1alpha1.StreamRun, error) {
	if file == "" {
		return nil, usagef("-f is required: the flow file, or - for stdin")
	}
	var data []byte
	var err error
	if file == "-" {
		data, err = io.ReadAll(e.Stdin)
	} else {
		data, err = os.ReadFile(file)
	}
	if err != nil {
		return nil, err
	}
	var r v1alpha1.StreamRun
	if err := yaml.UnmarshalStrict(data, &r); err != nil {
		return nil, hint(fmt.Errorf("%s: %w", file, err), "a flow file is one StreamRun; see config/samples/streamrun.yaml")
	}
	if want := v1alpha1.GroupVersion.String(); r.APIVersion != want || r.Kind != "StreamRun" {
		return nil, hint(fmt.Errorf("%s: is %s %s, not a StreamRun", file, r.APIVersion, r.Kind),
			"start the file with apiVersion: "+want+" and kind: StreamRun")
	}
	return &r, nil
}

// check runs the controller's own checks on a run: an invalid graph is an
// error, and images without a digest are warnings, since the controller
// refuses them unless it runs with -allow-tags (GO-26).
func check(file string, r *v1alpha1.StreamRun) (warnings []string, err error) {
	if err := controller.ValidateGraph(&r.Spec); err != nil {
		return nil, hint(fmt.Errorf("%s: %w", file, err), "fix the steps and edges in "+file)
	}
	for _, s := range r.Spec.Steps {
		if !controller.Pinned(s.Image) {
			warnings = append(warnings, fmt.Sprintf("step %s: image %s is not pinned by digest (@sha256:…); the controller refuses it unless it runs with -allow-tags", s.Name, s.Image))
		}
	}
	return warnings, nil
}

// namespaceFor puts the run in the namespace of -n, or of the context; a
// file that names another namespace than -n is an error, as in kubectl.
func namespaceFor(r *v1alpha1.StreamRun, o *opts, ns string) error {
	switch {
	case r.Namespace == "":
		r.Namespace = ns
	case o.kube.Namespace != "" && r.Namespace != o.kube.Namespace:
		return usagef("the file's namespace %s is not -n %s", r.Namespace, o.kube.Namespace)
	}
	return nil
}

// override changes one step of a flow before it is created: its image
// (GO-161) or its log level (GO-166), given as step=value.
func override(r *v1alpha1.StreamRun, flag string, values []string, set func(*v1alpha1.Step, string)) error {
	for _, v := range values {
		name, value, ok := strings.Cut(v, "=")
		if !ok || name == "" || value == "" {
			return usagef("--%s %s: want <step>=<value>", flag, v)
		}
		i := slices.IndexFunc(r.Spec.Steps, func(s v1alpha1.Step) bool { return s.Name == name })
		if i < 0 {
			var names []string
			for _, s := range r.Spec.Steps {
				names = append(names, s.Name)
			}
			return usagef("--%s %s: the flow has no step %q; its steps are %s", flag, v, name, strings.Join(names, ", "))
		}
		set(&r.Spec.Steps[i], value)
	}
	return nil
}

func runCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "run", "-f flow.yaml", &o)
	file := fs.String("f", "", "the flow file, or - for stdin")
	name := fs.String("name", "", "the run's name instead of metadata.name in the file, such as for a second run of the same flow")
	var images, levels stringList
	fs.Var(&images, "image", "<step>=<image> runs a step with another image, such as a fixed version; can be repeated")
	fs.Var(&levels, "log-level", "<step>=<level> sets a step's log level: debug, info, warn or error; debug shows more of what it does; can be repeated")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) > 0 {
		return usagef("run takes no arguments, only -f")
	}
	r, err := readFlow(e, *file)
	if err != nil {
		return err
	}
	if *name != "" {
		r.Name, r.GenerateName = *name, ""
	}
	if err := override(r, "image", images, func(s *v1alpha1.Step, v string) { s.Image = v }); err != nil {
		return err
	}
	if err := override(r, "log-level", levels, func(s *v1alpha1.Step, v string) { s.LogLevel = v }); err != nil {
		return err
	}
	for _, s := range r.Spec.Steps {
		if s.LogLevel != "" && !slices.Contains([]string{"debug", "info", "warn", "error"}, s.LogLevel) {
			return usagef("step %s: log level %q: want debug, info, warn or error", s.Name, s.LogLevel)
		}
	}
	warnings, err := check(*file, r)
	if err != nil {
		return err
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	if err := namespaceFor(r, &o, ns); err != nil {
		return err
	}
	for _, w := range warnings {
		fmt.Fprintln(e.Stderr, "warning:", w)
	}
	if err := c.Client.Create(ctx, r); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return hint(fmt.Errorf("streamrun %s already exists in namespace %s", r.Name, r.Namespace),
				"change metadata.name, use metadata.generateName, or delete the old run with kubectl delete streamrun "+r.Name)
		}
		return apiError(err)
	}
	return write(e.Stdout, o.output, typed(r), func(w io.Writer) {
		fmt.Fprintf(w, "streamrun %s/%s created\n", r.Namespace, r.Name)
		fmt.Fprintf(w, "follow it with: alpheus watch %s -n %s\n", r.Name, r.Namespace)
	})
}

// validation is what validate prints with -o.
type validation struct {
	File     string   `json:"file"`
	Valid    bool     `json:"valid"`
	Server   bool     `json:"server"`
	Warnings []string `json:"warnings"`
}

func validateCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "validate", "-f flow.yaml", &o)
	file := fs.String("f", "", "the flow file, or - for stdin")
	local := fs.Bool("local", false, "only the checks that need no cluster; skips the CRD's schema and rules")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) > 0 {
		return usagef("validate takes no arguments, only -f")
	}
	r, err := readFlow(e, *file)
	if err != nil {
		return err
	}
	warnings, err := check(*file, r)
	if err != nil {
		return err
	}
	if !*local {
		// The API server checks the CRD's schema, defaults and CEL rules
		// without creating anything.
		c, ns, err := e.connect(&o)
		if err != nil {
			return err
		}
		if err := namespaceFor(r, &o, ns); err != nil {
			return err
		}
		if err := c.Client.Create(ctx, r.DeepCopy(), client.DryRunAll); err != nil && !apierrors.IsAlreadyExists(err) {
			if apierrors.IsInvalid(err) {
				return hint(fmt.Errorf("%s: %w", *file, err), "fix the fields the message names")
			}
			return apiError(err)
		}
	}
	v := validation{File: *file, Valid: true, Server: !*local, Warnings: warnings}
	if v.Warnings == nil {
		v.Warnings = []string{}
	}
	return write(e.Stdout, o.output, v, func(w io.Writer) {
		for _, msg := range warnings {
			fmt.Fprintln(w, "warning:", msg)
		}
		if *local {
			fmt.Fprintf(w, "%s is valid (local checks only)\n", *file)
		} else {
			fmt.Fprintf(w, "%s is valid\n", *file)
		}
	})
}
