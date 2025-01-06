package jepsen

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"slices"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
	"github.com/bcrusu/scout/pkg/client"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_   utils.Lifecycle = (*Runner)(nil)
	log                 = logging.New("runner")
)

type Config struct {
	RunId           int           `validate:"required"`
	ClusterName     string        `validate:"required,maxLen:100"`
	SocketPath      string        `validate:"exists:socket"`
	OutputDir       string        `validate:"notExists"`
	Concurrency     int           `validate:"min:1"`
	Duration        time.Duration `validate:"min:1s"`
	ReadWriteRatio  float64       `validate:"min:0.1,max:10"`
	RequestRate     int           `validate:"min:1,max:1000"` // per second
	RequestMinKeys  int           `validate:"min:1,max:100"`
	RequestMaxKeys  int           `validate:"min:1,max:100"`
	NemesisDelay    time.Duration `validate:"min:1s"`
	NemesisInterval time.Duration `validate:"min:1s"`
	SlowDown        time.Duration `validate:"min:1ms"`
	NemesisEnabled  []string
	TruncateLogs    bool
}

type Runner struct {
	config     Config
	cluster    *cluster
	clients    []client.Client
	limiter    *rate.Limiter
	workload   *workload
	history    *history
	cancelFunc context.CancelFunc
}

type node struct {
	Id    string
	Ip    string
	Type  agent.ServiceType
	Agent *agent.Client
}

type cluster struct {
	All     []*node
	Control []*node
	Data    []*node
	API     []*node
}

type nemesis interface {
	Run(context.Context)
}

func NewRunner(ctx context.Context, config Config) *Runner {
	return &Runner{
		config: config,
	}
}

func (r *Runner) Start(ctx context.Context) error {
	if err := validation.Validate(r.config); err != nil {
		return err
	}

	cluster, err := r.getCluster(ctx)
	if err != nil {
		return err
	}

	clients, err := r.makeClients(cluster)
	if err != nil {
		return err
	}

	if err := r.resetNodes(ctx, cluster.All); err != nil {
		return err
	}

	if err := os.Mkdir(r.config.OutputDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create output dir")
	}

	// write test run config to disk for future reference
	if data, err := json.MarshalIndent(r.config, "", "    "); err != nil {
		return errors.Wrap(err, "failed to marshal config json")
	} else if err := os.WriteFile(path.Join(r.config.OutputDir, "config.json"), data, 0644); err != nil {
		return errors.Wrap(err, "failed to write config json")
	}

	historyFile, err := os.Create(path.Join(r.config.OutputDir, "history.json"))
	if err != nil {
		return errors.Wrap(err, "failed to create history file")
	}

	r.cluster = cluster
	r.clients = clients
	r.limiter = utils.NewRateLimiter(r.config.RequestRate, time.Second)
	r.workload = newWorkload(r.config)
	r.history = newHistory(historyFile)
	r.cancelFunc = utils.RunAsync(ctx, r.mainLoop)
	return nil
}

func (r *Runner) Stop() {
	r.cancelFunc()

	if err := r.history.Close(); err != nil {
		log.WithError(err).Error("Failed to close history.")
	}

	for _, client := range r.clients {
		client.Stop()
	}

	for _, n := range r.cluster.All {
		n.Agent.Close()
	}
}

func (r *Runner) mainLoop(parentCtx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timer := time.NewTimer(r.config.Duration)
	defer ticker.Stop()
	defer timer.Stop()

	ctx, cancel := context.WithCancel(parentCtx)
	nextId := 0
	doneCh := make(chan int)
	running := 0

	startWorker := func() {
		id := nextId
		worker, err := r.newWorker(id)
		nextId++
		if err != nil {
			log.WithError(err).Error("Failed to create worker.", "id", id)
			return
		}

		log.Info("Starting worker...", "id", id)
		running++
		go func() {
			if err := worker.Run(ctx); err != nil {
				log.WithError(err).Error("Worker failed.", "id", id)

				// slow down on internal errors
				if errors.Is(err, errors.InternalError) {
					time.Sleep(r.config.SlowDown)
				}
			} else {
				log.Info("Worker done.", "id", id)
			}
			doneCh <- id
		}()
	}

	startNemesis := func(instance nemesis, name string) {
		if !slices.Contains(r.config.NemesisEnabled, name) {
			return
		}

		running++
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(utils.AddJitter(r.config.NemesisDelay)):
				log.Info("Starting nemesis...", "name", name)
			}

			instance.Run(ctx)
			log.Info("Nemesis done.", "name", name)
			doneCh <- -666
		}()
	}

	for range r.config.Concurrency {
		startWorker()
	}

	startNemesis(r.newTimeNemesis())
	startNemesis(r.newServiceNemesis())

	for {
		select {
		case <-doneCh:
			running--
		case <-ticker.C:
			if running < r.config.Concurrency {
				startWorker()
			}
		case <-timer.C:
			utils.GracefulShutdown("Run duration elapsed.")
		case <-ctx.Done():
			cancel()

			for range running {
				<-doneCh
			}

			return
		}
	}
}

func (r *Runner) getCluster(ctx context.Context) (*cluster, error) {
	client, err := nodes.NewClient(r.config.SocketPath)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	resp, err := client.GetNodes(ctx, nil)
	if err != nil {
		return nil, err
	}

	result := &cluster{
		All: make([]*node, len(resp.Nodes)),
	}

	for i, node := range resp.Nodes {
		info, err := r.getNode(ctx, node)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get node %s info", node.Id)
		}

		result.All[i] = info

		switch info.Type {
		case agent.ServiceType_Control:
			result.Control = append(result.Control, info)
		case agent.ServiceType_Data:
			result.Data = append(result.Data, info)
		case agent.ServiceType_Api:
			result.API = append(result.API, info)
		default:
			return nil, errors.Errorf("unexpected node %s type %s", node.Id, info.Type)
		}
	}

	return result, nil
}

func (r *Runner) getNode(ctx context.Context, n *nodes.Node) (*node, error) {
	client, err := agent.NewClient(n.Ip)
	if err != nil {
		return nil, err
	}

	status, err := client.GetStatus(ctx, nil)
	if err != nil {
		return nil, err
	} else if !status.ServiceActive {
		return nil, errors.Error("service not active")
	}

	return &node{
		Id:    n.Id,
		Ip:    n.Ip,
		Type:  status.ServiceType,
		Agent: client,
	}, nil
}

func (r *Runner) newWorker(id int) (*worker, error) {
	return &worker{
		runId:    r.config.RunId,
		workerId: id,
		client:   r.clients[id%len(r.clients)],
		limiter:  r.limiter,
		workload: r.workload,
		history:  r.history.TxnWriter(id),
	}, nil
}

func (r *Runner) newTimeNemesis() (nemesis, string) {
	return &timeNemesis{
		cluster:  r.cluster,
		interval: r.config.NemesisInterval,
		history:  r.history.NemesisWriter("time_nemesis"),
		log:      logging.New("time_nemesis"),
	}, "time"
}

func (r *Runner) newServiceNemesis() (nemesis, string) {
	return &serviceNemesis{
		cluster:  r.cluster,
		interval: r.config.NemesisInterval,
		history:  r.history.NemesisWriter("service_nemesis"),
		log:      logging.New("service_nemesis"),
	}, "service"
}

func (r *Runner) makeClients(cluster *cluster) ([]client.Client, error) {
	// Round robin API servers just to balance the initial discovery stage, and because
	// the API server is not running in proxy mode, each client instance will individually
	// discover all other API servers and balance requests accordingly.

	result := make([]client.Client, r.config.Concurrency)

	for i := range r.config.Concurrency {
		idx := i % len(cluster.API)
		address := cluster.API[idx].Ip

		client, err := r.newClient(address)
		if err != nil {
			return nil, err
		}

		result[i] = client
	}

	return result, nil
}

func (r *Runner) newClient(address string) (client.Client, error) {
	client := client.New(
		client.WithClusterName(r.config.ClusterName),
		client.WithAddress(address))

	if err := client.Start(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to start API client")
	}

	return client, nil
}

func (c Config) Validate() error {
	if c.RequestMinKeys > c.RequestMaxKeys {
		return errors.Error("invalid RequestMinKeys/RequestMaxKeys fields")
	}
	return nil
}

func (r *Runner) resetNodes(ctx context.Context, nodes []*node) error {
	for _, node := range nodes {
		req := &agent.ResetRequest{
			Time:         timestamppb.Now(),
			Service:      false,
			Nemesis:      true,
			TruncateLogs: r.config.TruncateLogs,
		}

		if _, err := node.Agent.ResetService(ctx, req); err != nil {
			return errors.Wrapf(err, "failed to reset node %s", node.Id)
		}
	}

	return nil
}
