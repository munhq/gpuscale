package main

import (
	"flag"
	"os"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	gpucontroller "github.com/munhq/gpuscale/internal/controller"
	"github.com/munhq/gpuscale/internal/coordinator"
	gpumetrics "github.com/munhq/gpuscale/internal/metrics"
	"github.com/munhq/gpuscale/pkg/provider"
	"golang.org/x/time/rate"
	"github.com/munhq/gpuscale/pkg/provider/runpod"
	"github.com/munhq/gpuscale/pkg/provider/vastai"
	"github.com/munhq/gpuscale/pkg/provider/verda"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr            string
		healthProbeAddr        string
		batchWindow            time.Duration
		cooldownPeriod         time.Duration
		interruptionInterval   time.Duration
		workerMetricsInterval  time.Duration
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.DurationVar(&batchWindow, "batch-window", 10*time.Second, "Duration to batch pending pods before provisioning.")
	flag.DurationVar(&cooldownPeriod, "cooldown-period", 10*time.Minute, "Duration to wait before destroying idle nodes.")
	flag.DurationVar(&interruptionInterval, "interruption-poll-interval", 30*time.Second, "Interval for polling provider APIs for interruptions.")
	flag.DurationVar(&workerMetricsInterval, "worker-metrics-interval", 1*time.Minute, "Interval for scraping vLLM metrics from workers.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	log := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(log)

	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: healthProbeAddr,
	})
	if err != nil {
		setupLog.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	// Initialize provider registry
	registry := provider.NewRegistry()

	// Register providers based on environment variables
	if key := os.Getenv("VASTAI_API_KEY"); key != "" {
		registry.Register(vastai.New(key))
		setupLog.Info("Registered provider: vast.ai")
	}
	if clientID, clientSecret := os.Getenv("VERDA_CLIENT_ID"), os.Getenv("VERDA_CLIENT_SECRET"); clientID != "" && clientSecret != "" {
		registry.Register(verda.New(clientID, clientSecret))
		setupLog.Info("Registered provider: verda")
	}
	if key := os.Getenv("RUNPOD_API_KEY"); key != "" {
		registry.Register(runpod.New(key))
		setupLog.Info("Registered provider: runpod")
	}

	if len(registry.List()) == 0 {
		setupLog.Info("WARNING: No providers configured. Set VASTAI_API_KEY, VERDA_CLIENT_ID/VERDA_CLIENT_SECRET, or RUNPOD_API_KEY.")
	}

	// Dragonfly/Redis worker store for observability
	redisURL := os.Getenv("REDIS_URL")
	workerStore := gpucontroller.NewWorkerStore(redisURL)
	if workerStore != nil {
		setupLog.Info("Dragonfly worker store enabled", "url", redisURL)
	} else {
		setupLog.Info("Dragonfly worker store disabled (REDIS_URL not set)")
	}

	// Demand store — reads demand counters from Dragonfly DB 3 (maintained by GPU API)
	demandStore := gpucontroller.NewDemandStore(redisURL)
	if demandStore != nil {
		setupLog.Info("Demand store enabled (Dragonfly DB 3)", "url", redisURL)
	} else {
		setupLog.Info("Demand store disabled (no REDIS_URL or connection failed)")
	}

	// Ray capacity store — queries Ray cluster for GPU capacity
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "gpu-workloads"
	}
	prometheusURL := os.Getenv("PROMETHEUS_URL")
	if prometheusURL == "" {
		prometheusURL = "http://prometheus-operated.monitoring.svc.cluster.local:9090"
	}
	rayCapacityStore := gpucontroller.NewRayCapacityStore(mgr.GetClient(), namespace, prometheusURL)
	setupLog.Info("Ray capacity store enabled", "namespace", namespace, "prometheus", prometheusURL)

	// Create provisioning coordinator: centralized offer caching, blacklisting, rate limiting.
	coord := coordinator.NewCoordinator(registry, ctrl.Log.WithName("coordinator"), coordinator.Options{
		CacheTTL:         7 * time.Second,
		BlacklistTTL:     60 * time.Second,
		MaxAttempts:      5,
		OffersPerAttempt: 3,
		ProviderRates: map[string]rate.Limit{
			"vast.ai": 1, // 1 req/sec
			"verda":   3,
			"runpod":  3,
		},
	})

	// Set up controllers
	// ProvisioningController watches pending GPU pods (created by KEDA) and creates claims
	provisioningCtrl := gpucontroller.NewProvisioningController(
		mgr.GetClient(),
		ctrl.Log.WithName("provisioner"),
		batchWindow,
	)
	provisioningCtrl.DemandStore = demandStore
	provisioningCtrl.RayCapacityStore = rayCapacityStore
	if err := provisioningCtrl.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create provisioning controller")
		os.Exit(1)
	}

	disruptionCtrl := gpucontroller.NewDisruptionController(
		mgr.GetClient(),
		ctrl.Log.WithName("disruptor"),
		registry,
		cooldownPeriod,
	)
	disruptionCtrl.WorkerStore = workerStore
	disruptionCtrl.DemandStore = demandStore
	if err := disruptionCtrl.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create disruption controller")
		os.Exit(1)
	}

	claimReconciler := gpucontroller.NewClaimReconciler(
		mgr.GetClient(),
		ctrl.Log.WithName("claim-reconciler"),
		registry,
	)
	claimReconciler.Coordinator = coord
	claimReconciler.WorkerStore = workerStore
	if rayHead := os.Getenv("RAY_HEAD_ADDRESS"); rayHead != "" {
		claimReconciler.RayHeadAddress = rayHead
		setupLog.Info("Ray head address configured from env", "address", rayHead)
	}
	if err := claimReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create claim reconciler")
		os.Exit(1)
	}

	interruptionCtrl := gpucontroller.NewInterruptionController(
		mgr.GetClient(),
		ctrl.Log.WithName("interruption"),
		registry,
		interruptionInterval,
	)
	if err := interruptionCtrl.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create interruption controller")
		os.Exit(1)
	}

	// Health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	// Worker metrics collector — scrapes vLLM /metrics from Ready workers
	// and re-exposes them on the controller's :8080/metrics with provider labels.
	workerCollector := gpumetrics.NewWorkerMetricsCollector(mgr.GetClient(), workerMetricsInterval)
	ctrlmetrics.Registry.MustRegister(workerCollector)

	// Start collector as a background goroutine managed by the manager
	if err := mgr.Add(workerCollector); err != nil {
		setupLog.Error(err, "Unable to start worker metrics collector")
		os.Exit(1)
	}

	setupLog.Info("Starting GPUScale controller manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}
}
