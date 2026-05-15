package internal

import (
    "context"

    pipelinev1 "pipeline-controller/api/v1"
    corev1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/api/resource"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

type AdmissionChecker struct {
    client client.Client
}

func NewAdmissionChecker(c client.Client) *AdmissionChecker {
    return &AdmissionChecker{client: c}
}

func (a *AdmissionChecker) HasCapacity(ctx context.Context, reqCPU string, reqMemory string) (bool, error) {
    // NodeResources
    nodeList := &corev1.NodeList{}
    if err := a.client.List(ctx, nodeList); err != nil {
        return false, err
    }

    totalCPU := resource.NewQuantity(0, resource.DecimalSI)
    totalMemory := resource.NewQuantity(0, resource.BinarySI)

    for _, node := range nodeList.Items {
        if node.Spec.Unschedulable {
            continue
        }
        cpu := node.Status.Allocatable[corev1.ResourceCPU]
        mem := node.Status.Allocatable[corev1.ResourceMemory]
        totalCPU.Add(cpu)
        totalMemory.Add(mem)
    }

    // SUBMITTED / RUNNING job Resources
    jobList := &pipelinev1.PipelineJobList{}
    if err := a.client.List(ctx, jobList); err != nil {
        return false, err
    }

    for _, job := range jobList.Items {
        if job.Status.State == pipelinev1.StateSubmitted || job.Status.State == pipelinev1.StateRunning {
            usedCPU, err := resource.ParseQuantity(job.Spec.RequestCPU)
            if err != nil {
                continue
            }
            usedMem, err := resource.ParseQuantity(job.Spec.RequestMemory)
            if err != nil {
                continue
            }
            totalCPU.Sub(usedCPU)
            totalMemory.Sub(usedMem)
        }
    }

    neededCPU, err := resource.ParseQuantity(reqCPU)
    if err != nil {
        return false, err
    }
    neededMemory, err := resource.ParseQuantity(reqMemory)
    if err != nil {
        return false, err
    }

    cpuOK := totalCPU.Cmp(neededCPU) >= 0
    memOK := totalMemory.Cmp(neededMemory) >= 0

    return cpuOK && memOK, nil
}