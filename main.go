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

func main() {
    ctrl.SetLogger(zap.New())

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
    if err != nil {
        ctrl.Log.Error(err, "manager 啟動失敗")
        os.Exit(1)
    }

    // Initialize StorageChecker
    storage, err := internal.NewStorageChecker(context.Background())
    if err != nil {
        ctrl.Log.Error(err, "StorageChecker 初始化失敗")
        os.Exit(1)
    }

    // Initialize Reconciler，
    if err := (&internal.PipelineJobReconciler{
        Client:    mgr.GetClient(),
        DAG:       internal.NewDAGRegistry(),
        Admission: internal.NewAdmissionChecker(mgr.GetClient()),
        Eviction:  internal.NewEvictionManager(mgr.GetClient()),
        Storage:   storage,
    }).SetupWithManager(mgr); err != nil {
        ctrl.Log.Error(err, "Reconciler 註冊失敗")
        os.Exit(1)
    }

    ctrl.Log.Info("controller 啟動中...")
    if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
        ctrl.Log.Error(err, "controller 異常停止")
        os.Exit(1)
    }
}