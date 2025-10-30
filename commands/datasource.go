package commands

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"osctl/pkg/config"
	"osctl/pkg/kibana"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/spf13/cobra"
)

var dataSourceCmd = &cobra.Command{
	Use:   "datasource",
	Short: "Create Kibana data sources",
	Long: `Create and manage Kibana data sources for remote OpenSearch clusters.
Supports multitenancy and multidomain configurations.`,
	RunE: runDataSource,
}

func init() {
	addFlags(dataSourceCmd)
}

func runDataSource(cmd *cobra.Command, args []string) error {
	cfg := config.GetCommandConfig(cmd)

	dataSourceName := cfg.DataSourceName
	osdURL := cfg.OSDURL

	if dataSourceName == "" || cfg.OpenSearchURL == "" || osdURL == "" {
		return fmt.Errorf("dataSourceName, os-url and osd-url parameters are required")
	}

	logger := logging.NewLogger()
	_, err := opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	user := cfg.KibanaUser
	pass := cfg.KibanaPass
	kb := kibana.NewClient(osdURL, user, pass, cfg.GetTimeout())

	tenants := []string{"global"}
	if cfg.GetKibanaMultitenancy() {
		tf, err := config.GetConfig().GetTenantsConfig()
		if err != nil {
			return err
		}
		tenants = append(tenants, tf.GetTenantNames()...)
	}
	logger.Info(fmt.Sprintf("Tenants to process (%d): %s", len(tenants), strings.Join(tenants, ", ")))
	for _, tenant := range tenants {
		fr, err := kb.FindSavedObjects(tenant, "data-source", 10000)
		if err != nil {
			return err
		}
		exists := false
		existingTitles := []string{}
		for _, so := range fr.SavedObjects {
			if title, ok := so.Attributes["title"].(string); ok && title == dataSourceName {
				exists = true
			}
			if title, ok := so.Attributes["title"].(string); ok {
				existingTitles = append(existingTitles, title)
			}
		}
		logger.Info(fmt.Sprintf("Tenant %s existing data-sources (%d): %s", tenant, len(existingTitles), strings.Join(existingTitles, ", ")))
		if !exists {
			if err := kb.CreateDataSource(tenant, dataSourceName, cfg.OpenSearchURL, user, pass); err != nil {
				return err
			}
			logger.Info(fmt.Sprintf("Created data source in tenant %s", tenant))
		} else {
			logger.Info(fmt.Sprintf("Data source already exists in tenant %s (title=%s)", tenant, dataSourceName))
		}
	}

	if cfg.GetKibanaMultidomainEnabled() {
		remote := os.Getenv("REMOTE_CRT")
		parts := strings.Split(remote, "|")
		var concatenated string
		for _, p := range parts {
			tp := strings.TrimSpace(p)
			if tp == "" {
				continue
			}
			if b, err := base64.StdEncoding.DecodeString(tp); err == nil {
				concatenated += string(b)
			}
		}
		ns := cfg.KubeNamespace

		rc, err := rest.InClusterConfig()
		if err != nil {
			logger.Warn("InClusterConfig not available; skipping multidomain cert sync")
			return nil
		}
		cs, err := kubernetes.NewForConfig(rc)
		if err != nil {
			logger.Warn("Failed to init Kubernetes client; skipping multidomain cert sync")
			return nil
		}
		ctx := context.Background()

		if sec, err := cs.CoreV1().Secrets(ns).Get(ctx, "recoverer-certs", metav1.GetOptions{}); err == nil {
			if ca, ok := sec.Data["ca.crt"]; ok {
				concatenated += string(ca)
			}
		}

		desired := []byte(concatenated)
		existing, err := cs.CoreV1().Secrets(ns).Get(ctx, "multi-certs", metav1.GetOptions{})
		if err == nil {
			if cur, ok := existing.Data["multi.crt"]; ok {
				if strings.TrimSpace(string(cur)) == strings.TrimSpace(string(desired)) {
					logger.Info("multi-certs secret is up to date; nothing to do")
					return nil
				}
			}
			existing.Data = map[string][]byte{"multi.crt": desired}
			if _, err := cs.CoreV1().Secrets(ns).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
				return err
			}
		} else if apierrors.IsNotFound(err) {
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "multi-certs", Namespace: ns}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"multi.crt": desired}}
			if _, err := cs.CoreV1().Secrets(ns).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
				return err
			}
		} else {
			return err
		}

		dep, err := cs.AppsV1().Deployments(ns).Get(ctx, "kibana", metav1.GetOptions{})
		if err == nil {
			if dep.Spec.Template.Annotations == nil {
				dep.Spec.Template.Annotations = map[string]string{}
			}
			dep.Spec.Template.Annotations["osctl/restartedAt"] = time.Now().Format(time.RFC3339)
			if _, err := cs.AppsV1().Deployments(ns).Update(ctx, dep, metav1.UpdateOptions{}); err == nil {
				logger.Info("Updated multi-certs and restarted kibana")
			}
		}
	}
	return nil
}
