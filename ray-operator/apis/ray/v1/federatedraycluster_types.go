package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// FederatedRayClusterSpec describes one Ray runtime across private Kubernetes networks.
type FederatedRayClusterSpec struct {
	PrimaryCluster FederationPrimaryCluster `json:"primaryCluster"`
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	MemberClusters []FederationMemberCluster `json:"memberClusters"`
	Networking     FederationNetworking      `json:"networking"`
	// Delete waits for remote cleanup. Orphan explicitly releases remote resources.
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Delete
	// +optional
	MemberCleanupPolicy string `json:"memberCleanupPolicy,omitempty"`
}

// FederationPrimaryCluster configures the primary head, local workers, and the
// single autoscaler running on that head for all managed worker groups.
type FederationPrimaryCluster struct {
	HeadGroupSpec *HeadGroupSpec `json:"headGroupSpec"`
	// +optional
	RayVersion string `json:"rayVersion,omitempty"`
	// EnableInTreeAutoscaling runs one global autoscaler on the primary head.
	// Federation autoscaling currently requires Ray 2.56.0, autoscaler v2, and
	// kubeconfig credentials for every member.
	// +optional
	EnableInTreeAutoscaling *bool `json:"enableInTreeAutoscaling,omitempty"`
	// AutoscalerOptions configures the global autoscaler. The federation supplies
	// its command and args; version must be omitted or v2.
	// +optional
	AutoscalerOptions *AutoscalerOptions `json:"autoscalerOptions,omitempty"`
	// Replicas and scaleStrategy initialize new groups. Existing groups keep
	// their runtime targets in the primary RayCluster in both scaling modes.
	// +optional
	// +listType=map
	// +listMapKey=groupName
	// +kubebuilder:validation:items:XValidation:rule="!has(self.managedBy) || self.managedBy == 'ray.io/raycluster-controller'",message="workerGroups managedBy must be omitted or ray.io/raycluster-controller; federation assigns the generated group manager"
	WorkerGroups []WorkerGroupSpec `json:"workerGroups,omitempty"`
}

// FederationMemberCluster selects an existing remote namespace and optional credential.
// +kubebuilder:validation:XValidation:rule="has(self.kubeconfigSecretRef) ? (has(self.kubeconfigSecretRef.name) && size(self.kubeconfigSecretRef.name) > 0) : true",message="kubeconfigSecretRef requires a nonempty name"
// +kubebuilder:validation:XValidation:rule="has(self.kubeconfigSecretRef) ? (has(self.workerGroups) && size(self.workerGroups) > 0) : (!has(self.workerGroups) || size(self.workerGroups) == 0)",message="workerGroups must be configured only for managed members"
// +kubebuilder:validation:XValidation:rule="has(self.kubeconfigSecretRef) == has(oldSelf.kubeconfigSecretRef)",message="member management cannot change in place; remove the member before re-adding it"
type FederationMemberCluster struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// The Secret must be in the FRC namespace, labeled ray.io/federation-credential=true,
	// and contain a self-contained kubeconfig in data.kubeconfig.
	// Omit to manage the member manually. The federation never accesses that member API.
	// +optional
	KubeconfigSecretRef *corev1.LocalObjectReference `json:"kubeconfigSecretRef,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// Replicas and scaleStrategy initialize new managed groups; the primary
	// RayCluster owns their runtime targets after creation.
	// Targets are projected through the primary RayCluster to this managed member.
	// +listType=map
	// +listMapKey=groupName
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:XValidation:rule="!has(self.managedBy) || self.managedBy == 'ray.io/raycluster-controller'",message="workerGroups managedBy must be omitted or ray.io/raycluster-controller; federation assigns the generated group manager"
	// +optional
	WorkerGroups []WorkerGroupSpec `json:"workerGroups,omitempty"`
}

// FederationHeadEndpoint is a platform-managed stable private GCS endpoint.
type FederationHeadEndpoint struct {
	// Only UserProvided is initially supported; networking is externally managed.
	// +kubebuilder:validation:Enum=UserProvided
	// +kubebuilder:default=UserProvided
	// +optional
	Mode string `json:"mode,omitempty"`
	// Private IP address or DNS name, without a scheme or port.
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=6379
	// +optional
	GCSPort int32 `json:"gcsPort,omitempty"`
}

// FederationNetworking identifies the platform-managed head endpoint.
// Cross-cluster network connectivity is a platform prerequisite.
type FederationNetworking struct {
	HeadEndpoint FederationHeadEndpoint `json:"headEndpoint"`
}

// FederationWorkerGroupStatus is an aggregate observation, never a remote Pod inventory.
type FederationWorkerGroupStatus struct {
	GroupName          string `json:"groupName"`
	DesiredReplicas    int32  `json:"desiredReplicas"`
	ObservedReplicas   int32  `json:"observedReplicas"`
	ReadyReplicas      int32  `json:"readyReplicas"`
	PendingReplicas    int32  `json:"pendingReplicas"`
	FailedReplicas     int32  `json:"failedReplicas"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

// FederationMemberStatus retains managed cleanup destinations before any remote write.
// Manual members have no credential or observable remote capacity.
type FederationMemberStatus struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// RayClusterName is the managed member's actual RayCluster name, persisted
	// before remote writes. Existing members retain their original names.
	// Manual members do not have a controller-assigned RayCluster name.
	// +optional
	RayClusterName string `json:"rayClusterName,omitempty"`
	// RayClusterUID records the last child observed with this federation's ownership.
	// It distinguishes a failed create from a later change of ownership.
	// +optional
	RayClusterUID types.UID `json:"rayClusterUID,omitempty"`
	// ClusterUID is the member API's kube-system namespace UID, persisted before
	// remote writes. Credential rotation must preserve this identity, including
	// during cleanup; a Secret name or API server URL is not a cluster identity.
	// +optional
	ClusterUID types.UID `json:"clusterUID,omitempty"`
	// +optional
	KubeconfigSecretRef *corev1.LocalObjectReference `json:"kubeconfigSecretRef,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	LastUpdateTime *metav1.Time `json:"lastUpdateTime,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=groupName
	WorkerGroupStatuses []FederationWorkerGroupStatus `json:"workerGroupStatuses,omitempty"`
}

type FederatedRayClusterStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// User-provided endpoint published once the primary head is ready.
	// This reports primary head readiness, not cross-cluster reachability.
	// +optional
	HeadEndpoint *FederationHeadEndpoint `json:"headEndpoint,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=name
	MemberClusterStatuses []FederationMemberStatus `json:"memberClusterStatuses,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=frc
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +genclient
type FederatedRayCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              FederatedRayClusterSpec   `json:"spec"`
	Status            FederatedRayClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type FederatedRayClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FederatedRayCluster `json:"items"`
}
