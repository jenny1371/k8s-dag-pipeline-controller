package main

import (
	"context"
	"os"

	pipelinev1 "pipeline-controller/api/v1"
	"pipeline-controller/internal"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = runtime.NewScheme()

func init() {
	clientgoscheme.AddToScheme(scheme)
	pipelinev1.AddToScheme(scheme)
}

// leaderElectionNamespace is only needed when running outside the cluster;
// in-cluster controller-runtime detects the pod namespace itself.
func leaderElectionNamespace() string {
	if ns := os.Getenv("LEADER_ELECT_NAMESPACE"); ns != "" {
		return ns
	}
	return "default"
}

func main() {
	ctrl.SetLogger(zap.New())

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		// Enable with LEADER_ELECT=true to run more than one replica; only the leader reconciles.
		LeaderElection:          os.Getenv("LEADER_ELECT") == "true",
		LeaderElectionID:        "dag-pipeline-controller.pipeline.io",
		LeaderElectionNamespace: leaderElectionNamespace(),
	})
	if err != nil {
		ctrl.Log.Error(err, "failed to create manager")
		os.Exit(1)
	}

	storage, err := internal.NewStorageChecker(context.Background())
	if err != nil {
		ctrl.Log.Error(err, "failed to initialize StorageChecker")
		os.Exit(1)
	}

	if err := (&internal.PipelineJobReconciler{
		Client: mgr.GetClient(),
		// Admission decisions and the existing-Job check read straight from the API
		// server, so a job submitted a moment ago cannot be missed because of cache lag.
		APIReader: mgr.GetAPIReader(),
		DAG:       internal.NewDAGRegistry(),
		Admission: internal.NewAdmissionChecker(mgr.GetAPIReader()),
		Eviction:  internal.NewEvictionManager(mgr.GetClient()),
		Storage:   storage,
		Worker:    internal.NewWorkerConfigFromEnv(),
	}).SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "failed to register reconciler")
		os.Exit(1)
	}

	ctrl.Log.Info("controller starting")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "controller stopped unexpectedly")
		os.Exit(1)
	}
}
