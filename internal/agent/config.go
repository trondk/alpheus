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

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Config is what the agent of one replica of a step needs to know. The
// controller writes it as JSON from the StreamRun; storage and signal
// credentials are not part of it.
type Config struct {
	// Run, Namespace and UID identify the StreamRun.
	Run       string `json:"run"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
	// ResultsTTL is spec.resultsTTL, written to run.json (GO-98).
	ResultsTTL string `json:"resultsTTL,omitempty"`

	Step     string `json:"step"`
	Replica  int    `json:"replica"`
	Replicas int    `json:"replicas"`
	// Attempt is the attempt number, used as the manifests' epoch (GO-67).
	Attempt int64 `json:"attempt"`

	// Inputs are the step's incoming edges, and Outputs its outgoing ones.
	// An output that no edge leaves from is a result (SPEC 5.2).
	Inputs  []InputEdge  `json:"inputs,omitempty"`
	Outputs []OutputEdge `json:"outputs,omitempty"`

	// MaxBytes is spec.chunks.maxBytes; 0 means no limit (GO-105).
	MaxBytes int64 `json:"maxBytes,omitempty"`
	// MaxUnacked is spec.chunks.maxUnacked, per consumer replica (GO-35);
	// 0 means no limit.
	MaxUnacked int `json:"maxUnacked,omitempty"`
	// MaxOpen, Prefetch and InputOrder are from the step (GO-110 to
	// GO-112).
	MaxOpen    int    `json:"maxOpen,omitempty"`
	Prefetch   int    `json:"prefetch,omitempty"`
	InputOrder string `json:"inputOrder,omitempty"`

	// Socket is the Unix socket the component connects to.
	Socket string `json:"socket"`
	// Scratch is the folder chunks are kept in, shared with the component,
	// and ScratchLimit its size limit in bytes (GO-106); 0 means no limit.
	Scratch      string `json:"scratch"`
	ScratchLimit int64  `json:"scratchLimit,omitempty"`

	// Poll is how often manifests and ack files are read when no signal
	// comes (GO-40); default 5s.
	Poll Duration `json:"poll,omitempty"`
	// StartTimeout is how long the component may take to connect
	// (GO-29); default 5m.
	StartTimeout Duration `json:"startTimeout,omitempty"`
}

// InputEdge is an incoming edge: Edge is its name in storage, From the
// producing step with Replicas replicas, and Port the input it ends in.
type InputEdge struct {
	Edge     string `json:"edge"`
	From     string `json:"from"`
	Replicas int    `json:"replicas"`
	Port     string `json:"port"`
	// Side is a side input: every chunk, to every replica, again in each
	// attempt (GO-167).
	Side bool `json:"side,omitempty"`
}

// OutputEdge is an outgoing edge from one of the step's outputs to a step
// with Replicas replicas.
type OutputEdge struct {
	Edge     string `json:"edge"`
	Output   string `json:"output"`
	To       string `json:"to"`
	Replicas int    `json:"replicas"`
	// Side is a side input of the consumer, which does not hold back this
	// producer (GO-167).
	Side bool `json:"side,omitempty"`
}

// Input orders (GO-112).
const (
	OrderArrival       = "arrival"
	OrderDeterministic = "deterministic"
)

// Duration is a time.Duration written as a string such as "5s".
type Duration struct{ time.Duration }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

var namePattern = regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)

// validName reports whether s can name a step, output or port, as in the
// CRD.
func validName(s string) bool { return len(s) <= 20 && namePattern.MatchString(s) }

// ParseConfig decodes the config and fills in defaults.
func ParseConfig(data []byte) (Config, error) {
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("agent config: %w", err)
	}
	return c, c.complete()
}

func (c *Config) complete() error {
	if c.Replicas == 0 {
		c.Replicas = 1
	}
	if c.Attempt == 0 {
		c.Attempt = 1
	}
	if c.MaxOpen == 0 {
		c.MaxOpen = 8
	}
	if c.Prefetch == 0 {
		c.Prefetch = 2
	}
	if c.InputOrder == "" {
		c.InputOrder = OrderArrival
	}
	if c.Poll.Duration == 0 {
		c.Poll.Duration = 5 * time.Second
	}
	if c.StartTimeout.Duration == 0 {
		c.StartTimeout.Duration = 5 * time.Minute
	}
	for i := range c.Inputs {
		if c.Inputs[i].Replicas == 0 {
			c.Inputs[i].Replicas = 1
		}
		if c.Inputs[i].Port == "" {
			c.Inputs[i].Port = "default"
		}
	}
	for i := range c.Outputs {
		if c.Outputs[i].Replicas == 0 {
			c.Outputs[i].Replicas = 1
		}
		if c.Outputs[i].Output == "" {
			c.Outputs[i].Output = "default"
		}
	}

	var errs []error
	if c.Run == "" || c.UID == "" || !validName(c.Step) {
		errs = append(errs, fmt.Errorf("run, uid and step are required, got %q, %q, %q", c.Run, c.UID, c.Step))
	}
	if c.Replica < 0 || c.Replica >= c.Replicas {
		errs = append(errs, fmt.Errorf("replica %d of %d", c.Replica, c.Replicas))
	}
	if c.InputOrder != OrderArrival && c.InputOrder != OrderDeterministic {
		errs = append(errs, fmt.Errorf("inputOrder %q", c.InputOrder))
	}
	if c.Socket == "" || c.Scratch == "" {
		errs = append(errs, errors.New("socket and scratch are required"))
	}
	for _, in := range c.Inputs {
		if in.Edge == "" || !validName(in.From) || !validName(in.Port) {
			errs = append(errs, fmt.Errorf("input %+v", in))
		}
	}
	for _, out := range c.Outputs {
		if out.Edge == "" || !validName(out.Output) || !validName(out.To) {
			errs = append(errs, fmt.Errorf("output %+v", out))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("agent config: %w", errors.Join(errs...))
	}
	return nil
}
