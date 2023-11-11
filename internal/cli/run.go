package cli

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	"github.com/usedatabrew/benthos/v4/internal/bloblang/parser"
	"github.com/usedatabrew/benthos/v4/internal/cli/blobl"
	"github.com/usedatabrew/benthos/v4/internal/cli/common"
	"github.com/usedatabrew/benthos/v4/internal/cli/studio"
	clitemplate "github.com/usedatabrew/benthos/v4/internal/cli/template"
	"github.com/usedatabrew/benthos/v4/internal/cli/test"
	"github.com/usedatabrew/benthos/v4/internal/config"
	"github.com/usedatabrew/benthos/v4/internal/docs"
	"github.com/usedatabrew/benthos/v4/internal/filepath"
	"github.com/usedatabrew/benthos/v4/internal/filepath/ifs"
	"github.com/usedatabrew/benthos/v4/internal/template"
)

// Build stamps.
var (
	Version   = "unknown"
	DateBuilt = "unknown"
)

func init() {
	if Version != "unknown" {
		return
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, mod := range info.Deps {
			if mod.Path == "github.com/usedatabrew/benthos/v4" {
				if mod.Version != "(devel)" {
					Version = mod.Version
				}
				if mod.Replace != nil {
					v := mod.Replace.Version
					if v != "" && v != "(devel)" {
						Version = v
					}
				}
			}
		}
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && Version == "unknown" {
				Version = s.Value
			}
			if s.Key == "vcs.time" && DateBuilt == "unknown" {
				DateBuilt = s.Value
			}
		}
	}
}

//------------------------------------------------------------------------------

// App returns the full CLI app definition, this is useful for writing unit
// tests around the CLI.
func App() *cli.App {
	flags := []cli.Flag{
		&cli.BoolFlag{
			Name:    "version",
			Aliases: []string{"v"},
			Value:   false,
			Usage:   "display version info, then exit",
		},
		&cli.StringSliceFlag{
			Name:    "env-file",
			Aliases: []string{"e"},
			Value:   cli.NewStringSlice(),
			Usage:   "import environment variables from a dotenv file",
		},
		&cli.StringFlag{
			Name:  "log.level",
			Value: "",
			Usage: "override the configured log level, options are: off, error, warn, info, debug, trace",
		},
		&cli.StringSliceFlag{
			Name:    "set",
			Aliases: []string{"s"},
			Usage:   "set a field (identified by a dot path) in the main configuration file, e.g. `\"metrics.type=prometheus\"`",
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "",
			Usage:   "a path to a configuration file",
		},
		&cli.StringSliceFlag{
			Name:    "resources",
			Aliases: []string{"r"},
			Usage:   "pull in extra resources from a file, which can be referenced the same as resources defined in the main config, supports glob patterns (requires quotes)",
		},
		&cli.StringSliceFlag{
			Name:    "templates",
			Aliases: []string{"t"},
			Usage:   "EXPERIMENTAL: import Benthos templates, supports glob patterns (requires quotes)",
		},
		&cli.BoolFlag{
			Name:  "chilled",
			Value: false,
			Usage: "continue to execute a config containing linter errors",
		},
		&cli.BoolFlag{
			Name:    "watcher",
			Aliases: []string{"w"},
			Value:   false,
			Usage:   "EXPERIMENTAL: watch config files for changes and automatically apply them",
		},
	}

	app := &cli.App{
		Name:  "benthos",
		Usage: "A stream processor for mundane tasks - https://www.benthos.dev",
		Description: `
Either run Benthos as a stream processor or choose a command:

  benthos list inputs
  benthos create kafka//file > ./config.yaml
  benthos -c ./config.yaml
  benthos -r "./production/*.yaml" -c ./config.yaml`[1:],
		Flags: flags,
		Before: func(c *cli.Context) error {
			dotEnvPaths, err := filepath.Globs(ifs.OS(), c.StringSlice("env-file"))
			if err != nil {
				fmt.Printf("Failed to resolve env file glob pattern: %v\n", err)
				os.Exit(1)
			}
			for _, dotEnvFile := range dotEnvPaths {
				dotEnvBytes, err := ifs.ReadFile(ifs.OS(), dotEnvFile)
				if err != nil {
					fmt.Printf("Failed to read dotenv file: %v\n", err)
					os.Exit(1)
				}
				vars, err := parser.ParseDotEnvFile(dotEnvBytes)
				if err != nil {
					fmt.Printf("Failed to parse dotenv file: %v\n", err)
					os.Exit(1)
				}
				for k, v := range vars {
					if err = os.Setenv(k, v); err != nil {
						fmt.Printf("Failed to set env var '%v': %v\n", k, err)
						os.Exit(1)
					}
				}
			}

			templatesPaths, err := filepath.Globs(ifs.OS(), c.StringSlice("templates"))
			if err != nil {
				fmt.Printf("Failed to resolve template glob pattern: %v\n", err)
				os.Exit(1)
			}
			lints, err := template.InitTemplates(templatesPaths...)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Template file read error: %v\n", err)
				os.Exit(1)
			}
			if !c.Bool("chilled") && len(lints) > 0 {
				for _, lint := range lints {
					fmt.Fprintln(os.Stderr, lint)
				}
				fmt.Println("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
				os.Exit(1)
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			if c.Bool("version") {
				fmt.Printf("Version: %v\nDate: %v\n", Version, DateBuilt)
				os.Exit(0)
			}
			if c.Args().Len() > 0 {
				fmt.Fprintf(os.Stderr, "Unrecognised command: %v\n", c.Args().First())
				_ = cli.ShowAppHelp(c)
				os.Exit(1)
			}

			if code := common.RunService(c, Version, DateBuilt, false); code != 0 {
				os.Exit(code)
			}
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "echo",
				Usage: "Parse a config file and echo back a normalised version",
				Description: `
This simple command is useful for sanity checking a config if it isn't
behaving as expected, as it shows you a normalised version after environment
variables have been resolved:

  benthos -c ./config.yaml echo | less`[1:],
				Action: func(c *cli.Context) error {
					_, _, confReader := common.ReadConfig(c, false)
					conf, _, err := confReader.Read()
					if err != nil {
						fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
						os.Exit(1)
					}
					var node yaml.Node
					if err = node.Encode(conf); err == nil {
						sanitConf := docs.NewSanitiseConfig()
						sanitConf.RemoveTypeField = true
						sanitConf.ScrubSecrets = true
						err = config.Spec().SanitiseYAML(&node, sanitConf)
					}
					if err == nil {
						var configYAML []byte
						if configYAML, err = config.MarshalYAML(node); err == nil {
							fmt.Println(string(configYAML))
						}
					}
					if err != nil {
						fmt.Fprintf(os.Stderr, "Echo error: %v\n", err)
						os.Exit(1)
					}
					return nil
				},
			},
			lintCliCommand(),
			{
				Name:  "streams",
				Usage: "Run Benthos in streams mode",
				Description: `
Run Benthos in streams mode, where multiple pipelines can be executed in a
single process and can be created, updated and removed via REST HTTP
endpoints.

  benthos streams
  benthos -c ./root_config.yaml streams
  benthos streams ./path/to/stream/configs ./and/some/more
  benthos -c ./root_config.yaml streams ./streams/*.yaml

In streams mode the stream fields of a root target config (input, buffer,
pipeline, output) will be ignored. Other fields will be shared across all
loaded streams (resources, metrics, etc).

For more information check out the docs at:
https://benthos.dev/docs/guides/streams_mode/about`[1:],
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "no-api",
						Value: false,
						Usage: "Disable the HTTP API for streams mode",
					},
					&cli.BoolFlag{
						Name:  "prefix-stream-endpoints",
						Value: true,
						Usage: "Whether HTTP endpoints registered by stream configs should be prefixed with the stream ID",
					},
				},
				Action: func(c *cli.Context) error {
					os.Exit(common.RunService(c, Version, DateBuilt, true))
					return nil
				},
			},
			listCliCommand(),
			createCliCommand(),
			test.CliCommand(),
			clitemplate.CliCommand(),
			blobl.CliCommand(),
			studio.CliCommand(Version, DateBuilt),
		},
	}

	app.OnUsageError = func(context *cli.Context, err error, isSubcommand bool) error {
		fmt.Printf("Usage error: %v\n", err)
		_ = cli.ShowAppHelp(context)
		return err
	}
	return app
}

// Run the Benthos service, if the pipeline is started successfully then this
// call blocks until either the pipeline shuts down or a termination signal is
// received.
func Run(ctx context.Context) {
	_ = App().RunContext(ctx, os.Args)
}
