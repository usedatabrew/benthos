package io

import (
	"context"
	"path"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/usedatabrew/benthos/v4/internal/api"
	"github.com/usedatabrew/benthos/v4/internal/bundle"
	"github.com/usedatabrew/benthos/v4/internal/component/input"
	"github.com/usedatabrew/benthos/v4/internal/component/input/processors"
	"github.com/usedatabrew/benthos/v4/internal/docs"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newDynamicInput), docs.ComponentSpec{
		Name:    "dynamic",
		Summary: `A special broker type where the inputs are identified by unique labels and can be created, changed and removed during runtime via a REST HTTP interface.`,
		Footnotes: `
## Endpoints

### GET ` + "`/inputs`" + `

Returns a JSON object detailing all dynamic inputs, providing information such as their current uptime and configuration.

### GET ` + "`/inputs/{id}`" + `

Returns the configuration of an input.

### POST ` + "`/inputs/{id}`" + `

Creates or updates an input with a configuration provided in the request body (in YAML or JSON format).

### DELETE ` + "`/inputs/{id}`" + `

Stops and removes an input.

### GET ` + "`/inputs/{id}/uptime`" + `

Returns the uptime of an input as a duration string (of the form "72h3m0.5s"), or "stopped" in the case where the input has gracefully terminated.`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("inputs", "A map of inputs to statically create.").Map().HasDefault(map[string]any{}),
			docs.FieldString("prefix", "A path prefix for HTTP endpoints that are registered.").HasDefault(""),
		),
	})
	if err != nil {
		panic(err)
	}
}

func newDynamicInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	dynAPI := api.NewDynamic()

	inputs := map[string]input.Streamed{}
	for k, v := range conf.Dynamic.Inputs {
		iMgr := mgr.IntoPath("dynamic", "inputs", k)
		newInput, err := iMgr.NewInput(v)
		if err != nil {
			return nil, err
		}
		inputs[k] = newInput
	}

	inputConfigs := conf.Dynamic.Inputs
	inputConfigsMut := sync.RWMutex{}

	fanIn, err := newDynamicFanInInput(
		inputs, mgr.Logger(),
		func(ctx context.Context, l string) {
			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			uConf, exists := inputConfigs[l]
			if !exists {
				return
			}

			var confBytes []byte
			var node yaml.Node
			if err := node.Encode(uConf); err == nil {
				sanitConf := docs.NewSanitiseConfig()
				sanitConf.RemoveTypeField = true
				sanitConf.ScrubSecrets = true
				if err := docs.FieldInput("input", "").SanitiseYAML(&node, sanitConf); err == nil {
					confBytes, _ = yaml.Marshal(node)
				}
			}

			dynAPI.Started(l, confBytes)
			delete(inputConfigs, l)
		},
		func(ctx context.Context, l string) {
			dynAPI.Stopped(l)
		},
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(ctx context.Context, id string, c []byte) error {
		newConf := input.NewConfig()
		if err := yaml.Unmarshal(c, &newConf); err != nil {
			return err
		}
		iMgr := mgr.IntoPath("dynamic", "inputs", id)
		newInput, err := iMgr.NewInput(newConf)
		if err != nil {
			return err
		}
		inputConfigsMut.Lock()
		inputConfigs[id] = newConf
		inputConfigsMut.Unlock()
		if err = fanIn.SetInput(ctx, id, newInput); err != nil {
			mgr.Logger().Errorf("Failed to set input '%v': %v", id, err)
			inputConfigsMut.Lock()
			delete(inputConfigs, id)
			inputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(ctx context.Context, id string) error {
		err := fanIn.SetInput(ctx, id, nil)
		if err != nil {
			mgr.Logger().Errorf("Failed to close input '%v': %v", id, err)
		}
		return err
	})

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs/{id}/uptime"),
		`Returns the uptime of a specific input as a duration string, or "stopped" for inputs that are no longer running and have gracefully terminated.`,
		dynAPI.HandleUptime,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic inputs. For"+
			" more information read the `dynamic` input type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs"),
		"Get a map of running input identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanIn, nil
}
