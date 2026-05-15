package v1

import (
    runtime "k8s.io/apimachinery/pkg/runtime"
)

func (in *PipelineJob) DeepCopyObject() runtime.Object {
    if c := in.DeepCopy(); c != nil {
        return c
    }
    return nil
}

func (in *PipelineJob) DeepCopy() *PipelineJob {
    if in == nil {
        return nil
    }
    out := new(PipelineJob)
    in.DeepCopyInto(out)
    return out
}

func (in *PipelineJob) DeepCopyInto(out *PipelineJob) {
    *out = *in
    out.TypeMeta = in.TypeMeta
    in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
    in.Spec.DeepCopyInto(&out.Spec)
    out.Status = in.Status
}

func (in *PipelineJobSpec) DeepCopyInto(out *PipelineJobSpec) {
    *out = *in
    if in.Dependencies != nil {
        in, out := &in.Dependencies, &out.Dependencies
        *out = make([]string, len(*in))
        copy(*out, *in)
    }
}

func (in *PipelineJobList) DeepCopyObject() runtime.Object {
    if c := in.DeepCopy(); c != nil {
        return c
    }
    return nil
}

func (in *PipelineJobList) DeepCopy() *PipelineJobList {
    if in == nil {
        return nil
    }
    out := new(PipelineJobList)
    in.DeepCopyInto(out)
    return out
}

func (in *PipelineJobList) DeepCopyInto(out *PipelineJobList) {
    *out = *in
    out.TypeMeta = in.TypeMeta
    in.ListMeta.DeepCopyInto(&out.ListMeta)
    if in.Items != nil {
        in, out := &in.Items, &out.Items
        *out = make([]PipelineJob, len(*in))
        for i := range *in {
            (*in)[i].DeepCopyInto(&(*out)[i])
        }
    }
}