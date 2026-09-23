package federation

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// MemberClients restricts credentials to approved, namespace-local Secrets and
// rebuilds clients on every Secret rotation. No credentials are copied remotely.
type MemberClients struct {
	Reader client.Reader
	Scheme *runtime.Scheme
	mu     sync.Mutex
	cache  map[types.NamespacedName]cachedClient
}

type cachedClient struct {
	uid     types.UID
	version string
	client  client.Client
}

// Resolve identity from the API on every use, not from the credential cache. A
// kubeconfig's server may stay unchanged while DNS or a proxy is retargeted.
func (r *FederatedReconciler) memberCluster(ctx context.Context, namespace, secret string) (client.Client, types.UID, error) {
	remote, err := r.Members.Get(ctx, namespace, secret)
	if err != nil {
		return nil, "", err
	}
	identity := &corev1.Namespace{}
	if err := remote.Get(ctx, types.NamespacedName{Name: metav1.NamespaceSystem}, identity); err != nil {
		return nil, "", fmt.Errorf("cannot verify member cluster identity: %w", err)
	}
	if identity.UID == "" {
		return nil, "", fmt.Errorf("member cluster identity is empty")
	}
	return remote, identity.UID, nil
}

func (r *FederatedReconciler) boundMemberClient(ctx context.Context, namespace string, member rayv1.FederationMemberStatus) (client.Client, error) {
	if member.KubeconfigSecretRef == nil || member.ClusterUID == "" {
		return nil, fmt.Errorf("member %s has no persisted cluster identity", member.Name)
	}
	remote, uid, err := r.memberCluster(ctx, namespace, member.KubeconfigSecretRef.Name)
	if err != nil {
		return nil, err
	}
	if uid != member.ClusterUID {
		return nil, fmt.Errorf("member %s cluster identity changed; restore credentials for the original cluster before moving or cleaning up the member", member.Name)
	}
	return remote, nil
}

func (c *MemberClients) Get(ctx context.Context, namespace, name string) (client.Client, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := c.Reader.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("cannot read member credential Secret: %w", err)
	}
	if secret.Labels[CredentialLabel] != "true" {
		return nil, fmt.Errorf("member credential Secret requires administrator approval label %s=true", CredentialLabel)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.cache[key]; ok && entry.uid == secret.UID && entry.version == secret.ResourceVersion {
		return entry.client, nil
	}
	config, err := memberRESTConfig(secret.Data["kubeconfig"])
	if err != nil {
		return nil, err
	}
	remote, err := client.New(config, client.Options{Scheme: c.Scheme})
	if err != nil {
		return nil, fmt.Errorf("cannot initialize member Kubernetes client")
	}
	if c.cache == nil || len(c.cache) >= 128 {
		c.cache = map[types.NamespacedName]cachedClient{}
	}
	c.cache[key] = cachedClient{uid: secret.UID, version: secret.ResourceVersion, client: remote}
	return remote, nil
}

func memberRESTConfig(data []byte) (*rest.Config, error) {
	config, err := clientcmd.Load(data)
	if err != nil || len(data) == 0 {
		return nil, fmt.Errorf("invalid member kubeconfig")
	}
	// Kubeconfigs are data, never executable plugins or references to operator files.
	for _, auth := range config.AuthInfos {
		if auth.Exec != nil || auth.AuthProvider != nil || auth.TokenFile != "" || auth.ClientCertificate != "" || auth.ClientKey != "" {
			return nil, fmt.Errorf("member kubeconfig must embed credentials and cannot use exec, auth-provider, or local files")
		}
		if auth.Impersonate != "" || len(auth.ImpersonateGroups) > 0 || len(auth.ImpersonateUserExtra) > 0 {
			return nil, fmt.Errorf("member kubeconfig cannot impersonate users")
		}
	}
	for _, cluster := range config.Clusters {
		server, err := url.Parse(cluster.Server)
		if err != nil || server.Scheme != "https" || server.Host == "" || server.User != nil || cluster.InsecureSkipTLSVerify || cluster.CertificateAuthority != "" || cluster.ProxyURL != "" {
			return nil, fmt.Errorf("member kubeconfig requires verified HTTPS, embedded CA data, and no proxy")
		}
	}
	restConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("invalid member kubeconfig context or credentials")
	}
	restConfig.Timeout = 5 * time.Second
	restConfig.QPS, restConfig.Burst = 10, 20
	restConfig.UserAgent = "kuberay-federation"
	return restConfig, nil
}
