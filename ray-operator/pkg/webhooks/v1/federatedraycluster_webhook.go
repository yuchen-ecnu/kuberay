package v1

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	federationcontroller "github.com/ray-project/kuberay/ray-operator/controllers/federation"
)

func SetupFederatedRayClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.FederatedRayCluster{}).
		WithValidator(&FederatedRayClusterWebhook{Reader: mgr.GetAPIReader()}).Complete()
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-federatedraycluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=federatedrayclusters,verbs=create;update,versions=v1,name=vfederatedraycluster.kb.io,admissionReviewVersions=v1

type FederatedRayClusterWebhook struct {
	Reader client.Reader
}

var _ admission.Validator[*rayv1.FederatedRayCluster] = &FederatedRayClusterWebhook{}

func invalidFederation(frc *rayv1.FederatedRayCluster, err error) error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "FederatedRayCluster"},
		frc.Name,
		field.ErrorList{field.Invalid(field.NewPath("spec"), nil, err.Error())},
	)
}

func (w *FederatedRayClusterWebhook) ValidateCreate(_ context.Context, current *rayv1.FederatedRayCluster) (admission.Warnings, error) {
	if err := federationcontroller.ValidateFederation(current); err != nil {
		return nil, invalidFederation(current, err)
	}
	return nil, nil
}

func (w *FederatedRayClusterWebhook) ValidateUpdate(ctx context.Context, old, current *rayv1.FederatedRayCluster) (admission.Warnings, error) {
	if err := federationcontroller.ValidateFederation(current); err != nil {
		return nil, invalidFederation(current, err)
	}
	previous := federationcontroller.FederationPrimaryConfiguration(old.Spec)
	desired := federationcontroller.FederationPrimaryConfiguration(current.Spec)
	if err := federationcontroller.ValidateFederationPrimaryRevision(previous, desired); err != nil {
		// Check the accepted runtime, not an invalid or unapplied old FRC spec.
		// This permits safe legacy repair and rollback while preserving running
		// Pods. Reconciliation repeats the check before making any changes.
		if w.Reader != nil {
			err = (&federationcontroller.FederatedReconciler{Reader: w.Reader}).ValidatePrimaryUpdate(ctx, current)
		}
		if err != nil {
			return nil, invalidFederation(current, err)
		}
	}
	return nil, nil
}

func (w *FederatedRayClusterWebhook) ValidateDelete(_ context.Context, _ *rayv1.FederatedRayCluster) (admission.Warnings, error) {
	return nil, nil
}
