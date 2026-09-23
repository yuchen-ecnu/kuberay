package federation

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// Keep names within RayCluster's limit, including its reserved space for child
// resource names. Hash the complete name so truncation cannot alias two members.
func memberRayClusterName(federation, member string) string {
	name := federation + "-" + member
	if len(name) <= utils.MaxRayClusterNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	prefix := strings.TrimRight(name[:utils.MaxRayClusterNameLength-13], "-")
	return fmt.Sprintf("%s-%x", prefix, sum[:6])
}

// Prefer the readable name, but disambiguate different FRCs sharing a remote
// namespace. Once chosen, status is the authority; existing children never move.
func availableMemberName(ctx context.Context, remote client.Reader, frc *rayv1.FederatedRayCluster, member rayv1.FederationMemberCluster) (string, error) {
	name := memberRayClusterName(frc.Name, member.Name)
	sum := sha256.Sum256([]byte(string(frc.UID)))
	fallback := memberRayClusterName(name, fmt.Sprintf("%x", sum[:6]))
	for _, candidate := range []string{name, fallback} {
		cluster := &rayv1.RayCluster{}
		err := remote.Get(ctx, client.ObjectKey{Namespace: member.Namespace, Name: candidate}, cluster)
		if apierrors.IsNotFound(err) {
			return candidate, nil
		}
		if err != nil {
			return "", err
		}
		if cluster.Labels[OwnerLabel] == string(frc.UID) && cluster.Labels[utils.FederationMemberLabel] == member.Name && cluster.Spec.HeadGroupSpec == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("member RayCluster names are occupied by other owners")
}

// Upgrade the inventory without renaming an existing member or duplicating its
// workers. A legacy object belonging to another member cannot claim this entry.
func (r *FederatedReconciler) bindMemberName(ctx context.Context, frc *rayv1.FederatedRayCluster, member *rayv1.FederationMemberStatus) error {
	remote, err := r.boundMemberClient(ctx, frc.Namespace, *member)
	if err != nil {
		return err
	}
	legacy := &rayv1.RayCluster{}
	err = remote.Get(ctx, client.ObjectKey{Namespace: member.Namespace, Name: frc.Name}, legacy)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	name := memberRayClusterName(frc.Name, member.Name)
	if err == nil && legacy.Spec.HeadGroupSpec == nil && legacy.Labels[utils.FederationMemberLabel] == member.Name &&
		(legacy.Labels[OwnerLabel] == string(frc.UID) || legacy.Labels[orphanedByLabel] == string(frc.UID)) {
		name = legacy.Name
	}
	member.RayClusterName = name
	return nil
}

// Older alpha objects did not record an API identity. Bind them only when the
// original owned RayCluster can still be verified, and persist before proceeding.
// An absent object on an unbound API is not evidence of successful cleanup.
func (r *FederatedReconciler) bindLegacyMember(ctx context.Context, frc *rayv1.FederatedRayCluster, member *rayv1.FederationMemberStatus) error {
	remote, uid, err := r.memberCluster(ctx, frc.Namespace, member.KubeconfigSecretRef.Name)
	if err != nil {
		return err
	}
	cluster := &rayv1.RayCluster{}
	name := member.RayClusterName
	if name == "" {
		name = frc.Name
	}
	if err := remote.Get(ctx, client.ObjectKey{Namespace: member.Namespace, Name: name}, cluster); err != nil {
		return fmt.Errorf("cannot bind legacy member %s without verifying its original RayCluster: %w", member.Name, err)
	}
	if cluster.Spec.HeadGroupSpec != nil || cluster.Labels[OwnerLabel] != string(frc.UID) || cluster.Labels[utils.FederationMemberLabel] != member.Name {
		return fmt.Errorf("cannot bind legacy member %s: original RayCluster ownership does not match", member.Name)
	}
	member.ClusterUID = uid
	member.RayClusterName = name
	member.RayClusterUID = cluster.UID
	return nil
}
