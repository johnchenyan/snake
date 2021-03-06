package main

import (
	"context"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/lib/snakestar"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
	json "github.com/nikkolasg/hexjson"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	mux "github.com/gorilla/mux"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/ulimit"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/impl"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus miner process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "miner-api",
			Usage: "2345",
		},
		&cli.BoolFlag{
			Name:  "enable-gpu-proving",
			Usage: "enable use of GPU for mining operations",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "don't check full-node sync status",
		},
		&cli.BoolFlag{
			Name:  "manage-fdlimit",
			Usage: "manage open file limit",
			Value: true,
		},
		/* snake begin */
		&cli.BoolFlag{
			Name:  "pledge-sector",
			Usage: "pledge-sector",
		},
		&cli.BoolFlag{
			Name:  "window-post",
			Usage: "window-post",
		},
		&cli.BoolFlag{
			Name:  "winning-post",
			Usage: "winning-post",
		},
		&cli.StringFlag{
			Name:  "staged-sector",
			Usage: "specify the name of staged-sector, not path",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "commp-cache",
			Usage: "specify the name of commp-cache, not path",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "server-address",
			Usage: "get sealing sector id from server",
		},
		&cli.StringFlag{
			Name:  "seal",
			Usage: "use path for sealing",
		},
		&cli.StringFlag{
			Name:  "store",
			Usage: "use path for long-term storage",
		},
		/* snake end */
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			err := os.Setenv("BELLMAN_NO_GPU", "true")
			if err != nil {
				return err
			}
		}

		initCmdFlag(cctx) // snake add

		nodeApi, ncloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		defer ncloser()
		ctx := lcli.DaemonContext(cctx)

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("manage-fdlimit") {
			if _, _, err := ulimit.ManageFdLimit(); err != nil {
				log.Errorf("setting file descriptor limit: %s", err)
			}
		}

		//if v.APIVersion != build.FullAPIVersion {
		//	return xerrors.Errorf("lotus-daemon API version doesn't match: expected: %s", api.Version{APIVersion: build.FullAPIVersion})
		//} // snake del

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, nodeApi, false); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		minerRepoPath := cctx.String(FlagMinerRepo)
		r, err := repo.NewFS(minerRepoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized, run 'lotus-miner init' to set it up", minerRepoPath)
		}

		shutdownChan := make(chan struct{})

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Online(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("miner-api") },
				node.Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
					return multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + cctx.String("miner-api"))
				})),
			node.Override(new(api.FullNode), nodeApi),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		/* snake begin */
		seal := cctx.String("seal")
		store := cctx.String("store")

		if seal != "" && store != "" && seal == store {
			err = AttachStorage(minerapi, seal, true, true)
			if err != nil {
				return err
			}
		} else {
			if seal != "" {
				err = AttachStorage(minerapi, seal, true, false)
				if err != nil {
					return err
				}
			}

			if store != "" {
				err = AttachStorage(minerapi, store, false, true)
				if err != nil {
					return err
				}
			}
		}
		/* snake end */

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		// Bootstrap with full node
		remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
		if err != nil {
			return xerrors.Errorf("getting full node libp2p address: %w", err)
		}

		if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
			return xerrors.Errorf("connecting to full node (libp2p): %w", err)
		}

		log.Infof("Remote version %s", v)

		lst, err := manet.Listen(endpoint)
		if err != nil {
			return xerrors.Errorf("could not listen: %w", err)
		}

		mux := mux.NewRouter()

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filecoin", apistruct.PermissionedStorMinerAPI(metrics.MetricedStorMinerAPI(minerapi)))

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/remote").HandlerFunc(minerapi.(*impl.StorageMinerAPI).ServeRemote)
		mux.PathPrefix("/snake").HandlerFunc(minerapi.(*impl.StorageMinerAPI).ServeSnake) // snake add
		mux.PathPrefix("/").Handler(http.DefaultServeMux)                                 // pprof

		ah := &auth.Handler{
			Verify: minerapi.AuthVerify,
			Next:   mux.ServeHTTP,
		}

		srv := &http.Server{
			Handler: ah,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "lotus-miner"))
				return ctx
			},
		}

		sigChan := make(chan os.Signal, 2)
		go func() {
			select {
			case sig := <-sigChan:
				log.Warnw("received shutdown", "signal", sig)
			case <-shutdownChan:
				log.Warn("received shutdown")
			}

			log.Warn("Shutting down...")
			if err := stop(context.TODO()); err != nil {
				log.Errorf("graceful shutting down failed: %s", err)
			}
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

		return srv.Serve(manet.NetListener(lst))
	},
}

/* snake begin */
func initCmdFlag(cctx *cli.Context) error {
	if cctx.Bool("pledge-sector") {
		snakestar.PledgeSector = true
	}

	if cctx.Bool("winning-post") {
		snakestar.WinningPost = true
	}
	if cctx.Bool("window-post") {
		snakestar.WindowPost = true
	}

	if cctx.Bool("pledge-sector") {
		snakestar.StagedPath = cctx.String("staged-sector")
		snakestar.CommpCache = cctx.String("commp-cache")
		if snakestar.StagedPath == "" || snakestar.CommpCache == "" {
			log.Error("staged-sector or commp-cache not provided")
			panic("staged-sector or commp-cache not provided")
		}
	}

	if cctx.String("server-address") != "" {
		snakestar.ServerAddress = cctx.String("server-address")
	}

	return nil
}

func AttachStorage(api api.StorageMiner, path string, seal bool, store bool) error {
	p, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding path: %w", err)
	}
	_, err = os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(p, 0755); err != nil {
				if !os.IsExist(err) {
					return err
				}
			}
		}
	} else {
		//return nil
	}

	metaFileAreadyExised := false

	_, err = os.Stat(filepath.Join(p, metaFile))
	if !os.IsNotExist(err) {
		if err == nil {
			metaFileAreadyExised = true
			//return nil
		} else {
			return err
		}
	}
	if !metaFileAreadyExised {
		var cfg = &stores.LocalStorageMeta{
			ID:       stores.ID(uuid.New().String()),
			Weight:   10,
			CanSeal:  seal,
			CanStore: store,
		}
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return xerrors.Errorf("marshaling storage config: %w", err)
		}

		if err := ioutil.WriteFile(filepath.Join(p, metaFile), b, 0644); err != nil {
			return xerrors.Errorf("persisting storage metadata (%s): %w", filepath.Join(p, metaFile), err)
		}
		return api.StorageAddLocal(context.TODO(), p)
	} else {
		local, err := api.StorageLocal(context.TODO())
		if err != nil {
			return xerrors.Errorf("StorageLocal Error: %v", err)
		} else {
			for _, p := range local {
				if p == path {
					return nil
				}
			}
			return api.StorageAddLocal(context.TODO(), p)
		}
	}
}

/* snake end */
