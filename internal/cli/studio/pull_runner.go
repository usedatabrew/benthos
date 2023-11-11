package studio

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"runtime/pprof"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/urfave/cli/v2"

	ibloblang "github.com/usedatabrew/benthos/v4/internal/bloblang"
	"github.com/usedatabrew/benthos/v4/internal/bundle"
	"github.com/usedatabrew/benthos/v4/internal/bundle/tracing"
	"github.com/usedatabrew/benthos/v4/internal/cli/common"
	"github.com/usedatabrew/benthos/v4/internal/cli/studio/metrics"
	stracing "github.com/usedatabrew/benthos/v4/internal/cli/studio/tracing"
	"github.com/usedatabrew/benthos/v4/internal/config"
	"github.com/usedatabrew/benthos/v4/internal/docs"
	"github.com/usedatabrew/benthos/v4/internal/filepath/ifs"
	"github.com/usedatabrew/benthos/v4/internal/log"
	"github.com/usedatabrew/benthos/v4/internal/manager"
	"github.com/usedatabrew/benthos/v4/internal/stream"
	"github.com/usedatabrew/benthos/v4/public/bloblang"
)

type noopStopper struct{}

func (n noopStopper) Stop(_ context.Context) error {
	return nil
}

// When a stream component (manager with resources or stream running a config)
// is instructed to shutdown this deadline determines the maximum amount of time
// we're willing to wait for it to be done gracefully when otherwise not
// configured.
const defaultCloseDeadline = time.Second * 30

// PullRunner encapsulates a component that runs a Benthos stream continuously
// by obtaining a deployment allocation from a Studio session, pulling the
// configs from that deployment, and then executing the configs in the
// background.
//
// Each time Sync is called the runner will poll the session for any deployment
// reallocations, or config changes and attempt to reflect those changes in the
// running stream.
type PullRunner struct {
	localConf      config.Type
	confReader     *config.Reader
	sessionTracker *sessionTracker

	// Controls disabled deployment rotations
	isDisabled     bool
	latestMainConf *stream.Config

	metricsFlushPeriod time.Duration
	metrics            *metrics.Tracker
	mgr                bundle.NewManagement
	tracingSummary     *tracing.Summary
	stoppableMgr       *common.StoppableManager
	stoppableStream    *common.SwappableStopper
	logger             log.Modular

	exitDelay   time.Duration
	exitTimeout time.Duration

	cliContext  *cli.Context
	setList     []string
	strictMode  bool
	version     string
	dateBuilt   string
	allowTraces bool

	nowFn func() time.Time
}

// OptSetNowFn sets the function used to obtain a new time value representing
// now. By default time.Now is used.
func OptSetNowFn(fn func() time.Time) func(*PullRunner) {
	return func(pr *PullRunner) {
		pr.nowFn = fn
	}
}

// NewPullRunner creates a new PullRunner from a cli context, which is used for
// overriding a range of stream behaviours and settings various studio specific
// details such as the endpoint. The version, date stamps must be provided as
// well as a valid token and secret for the session that will be accessed.
//
// It's odd having to push a *cli.Context through here but I wanted to avoid
// needing to pass tens of parameters through for things like --set,
// --prefix-stream-endpoints, etc. Some of those customisation options are
// pushed deep into things like the manager constructor, and as cli options are
// expanded it'd be a drag to have to update every single constructor signature
// that calls into it.
func NewPullRunner(c *cli.Context, version, dateBuilt, token, secret string, opts ...func(p *PullRunner)) (*PullRunner, error) {
	r := &PullRunner{
		localConf:          config.New(),
		metricsFlushPeriod: time.Second * 30,
		stoppableStream:    common.NewSwappableStopper(&noopStopper{}),
		cliContext:         c,
		setList:            c.StringSlice("set"),
		strictMode:         !c.Bool("chilled"),
		version:            version,
		dateBuilt:          dateBuilt,
		nowFn:              time.Now,
		allowTraces:        c.Bool("send-traces"),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.metrics = metrics.NewTracker(metrics.OptSetNowFn(r.nowFn))

	nodeName := c.String("name")
	if nodeName == "" {
		var err error
		if nodeName, err = gonanoid.New(); err != nil {
			return nil, fmt.Errorf("failed to generate name: %w", err)
		}
	}

	baseURL, err := url.Parse(c.String("endpoint"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %w", err)
	}
	baseURL.Path = path.Join(baseURL.Path, fmt.Sprintf("/api/v1/node/session/%v", c.String("session")))

	var localLints []string
	if localConfPath := c.String("config"); localConfPath != "" {
		localReader := config.NewReader(localConfPath, c.StringSlice("resources"),
			config.OptAddOverrides(r.setList...),
			config.OptTestSuffix("_benthos_test"),
		)
		if r.localConf, localLints, err = localReader.Read(); err != nil {
			return nil, fmt.Errorf("configuration file read error: %w", err)
		}
		_ = r.withExitContext(c.Context, func(ctx context.Context) error {
			return localReader.Close(ctx)
		})
	}

	// Logger is suuuuper primitive so we only instantiate it from the local
	// config and cli args.
	if r.logger, err = common.CreateLogger(c, r.localConf, false); err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	r.logLints(localLints)
	if r.strictMode && len(localLints) > 0 {
		return nil, errors.New("linter errors were found in local configuration files, to ignore these errors run Benthos with --chilled")
	}

	if r.sessionTracker, err = initSessionTracker(c.Context, r.nowFn, r.logger, nodeName, baseURL.String(), token, secret); err != nil {
		return nil, fmt.Errorf("failed to initialise session connection: %w", err)
	}
	r.metricsFlushPeriod = r.sessionTracker.MetricsGuideFlushPeriod()

	err = r.bootstrapConfigReader(c.Context)
	if err != nil {
		r.logger.Errorf("Failed to run initial sync config: %v", err)
	}
	r.sessionTracker.SetRunError(err)
	return r, nil
}

func (r *PullRunner) logLints(lints []string) {
	for _, lint := range lints {
		if r.strictMode {
			r.logger.With("lint", lint).Errorln("Config lint error")
		} else {
			r.logger.With("lint", lint).Warnln("Config lint error")
		}
	}
}

func (r *PullRunner) setStreamDisabled(ctx context.Context, toDisabled bool) error {
	if r.isDisabled == toDisabled {
		return nil // Already set
	}

	return r.withExitContext(ctx, func(ctx context.Context) error {
		if toDisabled {
			if err := r.stoppableStream.Replace(ctx, func() (common.Stoppable, error) {
				return &noopStopper{}, nil
			}); err != nil {
				return err
			}
		} else if r.latestMainConf != nil && r.mgr != nil {
			if err := r.stoppableStream.Replace(ctx, func() (common.Stoppable, error) {
				return stream.New(*r.latestMainConf, r.mgr)
			}); err != nil {
				return err
			}
		}
		r.isDisabled = toDisabled
		return nil
	})
}

func (r *PullRunner) triggerStreamReset(ctx context.Context, conf *config.Type, mgr bundle.NewManagement) error {
	r.latestMainConf = &conf.Config
	if r.isDisabled {
		return nil
	}
	return r.withExitContext(ctx, func(ctx context.Context) error {
		return r.stoppableStream.Replace(ctx, func() (common.Stoppable, error) {
			return stream.New(conf.Config, mgr)
		})
	})
}

func (r *PullRunner) bootstrapConfigReader(ctx context.Context) (bootstrapErr error) {
	var initMainFile string
	var initResources []string
	initFiles := r.sessionTracker.Files()
	if initFiles.MainConfig != nil {
		initMainFile = initFiles.MainConfig.Name
	}
	for _, f := range initFiles.ResourceConfigs {
		initResources = append(initResources, f.Name)
	}

	sessFS := &sessionFS{
		tracker: r.sessionTracker,
		backup:  ifs.OS(),
	}

	bloblEnv := ibloblang.GlobalEnvironment().WithCustomImporter(func(name string) ([]byte, error) {
		return ifs.ReadFile(sessFS, name)
	})

	lintConf := docs.NewLintConfig()
	lintConf.BloblangEnv = bloblang.XWrapEnvironment(bloblEnv).Deactivated()

	confReaderTmp := config.NewReader(initMainFile, initResources,
		config.OptSetBootstrapConfig(&r.localConf),
		config.OptAddOverrides(r.setList...),
		config.OptTestSuffix("_benthos_test"),
		config.OptUseFS(sessFS),
		config.OptSetLintConfig(lintConf),
	)

	defer func() {
		if bootstrapErr != nil {
			_ = r.withExitContext(ctx, func(ctx context.Context) error {
				return confReaderTmp.Close(ctx)
			})
		}
	}()

	conf, lints, err := confReaderTmp.Read()
	if err != nil {
		return fmt.Errorf("failed bootstrap config read: %w", err)
	}
	r.logLints(lints)
	if r.strictMode && len(lints) > 0 {
		return errors.New("found linting errors in config")
	}

	tmpEnv, tmpTracingSummary := tracing.TracedBundle(bundle.GlobalEnvironment)
	tmpTracingSummary.SetEnabled(false)

	stopMgrTmp, err := common.CreateManager(
		r.cliContext, r.logger, false, r.version, r.dateBuilt, conf,
		manager.OptSetEnvironment(tmpEnv),
		manager.OptSetBloblangEnvironment(bloblEnv),
		manager.OptSetFS(sessFS))
	if err != nil {
		return fmt.Errorf("failed to create manager from bootstrap config: %w", err)
	}
	defer func() {
		if bootstrapErr != nil {
			_ = r.withExitContext(ctx, func(ctx context.Context) error {
				return stopMgrTmp.Stop(ctx)
			})
		}
	}()

	mgrTmp := stopMgrTmp.Manager().WithAddedMetrics(r.metrics)
	if err := r.triggerStreamReset(ctx, &conf, mgrTmp); err != nil {
		return fmt.Errorf("failed initial stream reset: %w", err)
	}

	// Extract shutdown timeout values
	var exitDelay time.Duration
	if td := conf.SystemCloseDelay; len(td) > 0 {
		var err error
		if exitDelay, err = time.ParseDuration(td); err != nil {
			return fmt.Errorf("failed to parse shutdown delay period string: %w", err)
		}
	}

	var exitTimeout time.Duration
	if tout := conf.SystemCloseTimeout; len(tout) > 0 {
		var err error
		if exitTimeout, err = time.ParseDuration(tout); err != nil {
			return fmt.Errorf("failed to parse shutdown timeout period string: %w", err)
		}
	}

	r.stoppableMgr = stopMgrTmp
	r.mgr = mgrTmp
	r.tracingSummary = tmpTracingSummary
	r.confReader = confReaderTmp
	r.exitDelay = exitDelay
	r.exitTimeout = exitTimeout

	if err := confReaderTmp.SubscribeConfigChanges(func(conf *config.Type) error {
		return r.triggerStreamReset(context.Background(), conf, mgrTmp)
	}); err != nil {
		return fmt.Errorf("failed to subscribe to config changes: %w", err)
	}
	return
}

// Sync with the target session, obtaining new allocations, config changes,
// passing errors and metrics, etc.
func (r *PullRunner) Sync(ctx context.Context) {
	var metricsOut *metrics.Observed
	if r.nowFn().Sub(r.metrics.LastFlushed()) > r.metricsFlushPeriod {
		metricsOut = r.metrics.Flush()
	}

	// Pause traces (if previously enabled), and flush all events collected
	// since the last sync.
	var tracingOut *stracing.Observed
	if r.tracingSummary != nil {
		r.tracingSummary.SetEventLimit(0)
		r.tracingSummary.SetEnabled(false)
		if r.allowTraces {
			tracingOut = stracing.FromInternal(r.tracingSummary)
		}
	}

	isDisabled, diff, requestedTraces, err := r.sessionTracker.Sync(ctx, metricsOut, tracingOut)
	if err != nil {
		r.logger.Errorf("Failed session sync: %v", err)
		return
	}

	if r.confReader == nil {
		// We haven't bootstrapped yet, likely due to a bad config on
		// our first attempt. The latest sync may have fixed the issue
		// so we can potentially try again.

		if isDisabled {
			// Except the deployment is disabled now, so don't.
			r.logger.Infoln("Deployment is disabled, so skipping bootstrap of initial config")
			return
		}

		err := r.bootstrapConfigReader(ctx)
		if err != nil {
			r.logger.Errorf("Failed to bootstrap initial config: %v", err)
		}
		r.sessionTracker.SetRunError(err)
		return
	}

	if err = r.setStreamDisabled(ctx, isDisabled); err != nil {
		r.logger.Errorf("Failed to toggle deployment enablement: %v", err)
		return
	}

	var runErr error // TODO: Use new multi error
	if diff != nil {
		// We've already bootstrapped, and so we need to update our
		// config reader of all changes.
		for _, resName := range diff.RemoveResources {
			if err := r.confReader.TriggerResourceDelete(r.mgr, resName); err != nil {
				r.logger.Errorf("Failed to reflect resource file '%v' deletion: %v", r, err)
				runErr = err
			}
		}
		for _, res := range diff.AddResources {
			if err := r.confReader.TriggerResourceUpdate(r.mgr, r.strictMode, res.Name); err != nil {
				r.logger.Errorf("Failed to reflect resource file '%v' update: %v", res.Name, err)
				runErr = err
			}
		}
		if diff.MainConfig != nil {
			if err := r.confReader.TriggerMainUpdate(r.mgr, r.strictMode, diff.MainConfig.Name); err != nil {
				r.logger.Errorf("Failed to reflect main config file '%v' update: %v", diff.MainConfig.Name, err)
				runErr = err
			}
		}
		r.sessionTracker.SetRunError(runErr)
	}
	if runErr != nil {
		return
	}

	// Set a new trace limit and re-enable if appropriate, we want to do this if
	// either the files we already have match the deployment, or after we've
	// successfully followed the diff.
	if r.allowTraces {
		r.tracingSummary.SetEventLimit(requestedTraces)
		r.tracingSummary.SetEnabled(requestedTraces > 0)
	}
}

func (r *PullRunner) withExitContext(ctx context.Context, fn func(context.Context) error) error {
	tout := r.exitTimeout
	if tout <= 0 {
		tout = defaultCloseDeadline
	}
	ctx, done := context.WithTimeout(ctx, tout)
	defer done()
	return fn(ctx)
}

// Stop any underlying stream and managers that may exist.
func (r *PullRunner) Stop(ctx context.Context) error {
	{
		// Use a shorter deadline for leaving as it's optional
		leaveCtx := ctx
		if dl, exists := ctx.Deadline(); !exists || dl.Sub(r.nowFn()) > time.Second {
			var done func()
			leaveCtx, done = context.WithTimeout(leaveCtx, time.Second)
			defer done()
		}
		if err := r.sessionTracker.Leave(leaveCtx); err != nil {
			r.logger.Warnf("Failed to inform Studio session that we're shutting down: %v", err)
		}
	}

	if r.exitDelay > 0 {
		r.logger.Infof("Shutdown delay is in effect for %s", r.exitDelay)
		if err := common.DelayShutdown(ctx, r.exitDelay); err != nil {
			r.logger.Errorf("Shutdown delay failed: %s", err)
		}
	}

	return r.withExitContext(ctx, func(ctx context.Context) error {
		if err := r.stoppableStream.Stop(ctx); err != nil {
			r.logger.Warnf(
				"Service failed to close the running stream cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr\n", err,
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			return err
		}
		if r.stoppableMgr == nil {
			return nil
		}
		if err := r.stoppableMgr.Stop(ctx); err != nil {
			r.logger.Warnf(
				"Service failed to close resources cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr\n", err,
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			return err
		}
		return nil
	})
}
