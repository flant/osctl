package commands

import (
	"context"
	"encoding/base64"
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/kibana"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
	"slices"
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

	if osdURL != "" && !strings.HasPrefix(osdURL, "http://") && !strings.HasPrefix(osdURL, "https://") {
		osdURL = "https://" + osdURL
	}

	if dataSourceName == "" || cfg.OpenSearchURL == "" || osdURL == "" {
		return fmt.Errorf("dataSourceName, os-url and osd-url parameters are required")
	}

	logger := logging.NewLogger()
	dryRun := cfg.GetDryRun()
	_, err := utils.NewOSClientFromCommandConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	user := cfg.KibanaUser
	pass := cfg.KibanaPass
	kb := kibana.NewClient(osdURL, user, pass, cfg.GetTimeout())

	tenants := []string{"global"}
	tenantNamesForLog := []string{"global"}
	if cfg.GetDataSourceKibanaMultitenancy() {
		tf, err := config.GetConfig().GetTenantsConfig()
		if err != nil {
			return err
		}
		for _, t := range tf.Tenants {
			normalizedName := utils.NormalizeTenantName(t.Name)
			tenants = append(tenants, normalizedName)
			tenantNamesForLog = append(tenantNamesForLog, t.Name)
		}
	}
	logger.Info(fmt.Sprintf("Tenants to process (%d): %s", len(tenants), strings.Join(tenantNamesForLog, ", ")))
	for i, tenant := range tenants {
		tenantNameForLog := tenantNamesForLog[i]
		existingTitles, err := getTenantDataSourceTitles(kb, tenant)
		if err != nil {
			return err
		}
		exists := slices.Contains(existingTitles, dataSourceName)
		logger.Info(fmt.Sprintf("Tenant %s existing data-sources (%d): %s", tenantNameForLog, len(existingTitles), strings.Join(existingTitles, ", ")))
		if !exists {
			if dryRun {
				logger.Info(fmt.Sprintf("DRY RUN: Would create data source in tenant %s", tenantNameForLog))
			} else {
				if err := kb.CreateDataSource(tenant, dataSourceName, cfg.OpenSearchURL, user, pass); err != nil {
					return err
				}
				logger.Info(fmt.Sprintf("Created data source in tenant %s", tenantNameForLog))
			}
		} else {
			logger.Info(fmt.Sprintf("Data source already exists in tenant %s (title=%s)", tenantNameForLog, dataSourceName))
		}
	}

	if cfg.GetDataSourceKibanaMultidomainEnabled() {
		remote := cfg.DataSourceRemoteCRT
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
			if dryRun {
				logger.Info("DRY RUN: Would update secret multi-certs with new multi.crt contents")
			} else {
				existing.Data = map[string][]byte{"multi.crt": desired}
				if _, err := cs.CoreV1().Secrets(ns).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}
		} else if apierrors.IsNotFound(err) {
			if dryRun {
				logger.Info("DRY RUN: Would create secret multi-certs with multi.crt contents")
			} else {
				sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "multi-certs", Namespace: ns}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"multi.crt": desired}}
				if _, err := cs.CoreV1().Secrets(ns).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
					return err
				}
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
			if dryRun {
				logger.Info("DRY RUN: Would update multi-certs and restart kibana")
			} else {
				if _, err := cs.AppsV1().Deployments(ns).Update(ctx, dep, metav1.UpdateOptions{}); err == nil {
					logger.Info("Updated multi-certs and restarted kibana")
				}
			}
		}
	}
	return nil
}

func getTenantDataSourceTitles(kb *kibana.Client, tenant string) ([]string, error) {
	fr, err := kb.FindSavedObjects(tenant, "data-source", 10000)
	if err != nil {
		return nil, err
	}
	titles := []string{}
	for _, so := range fr.SavedObjects {
		if title, ok := so.Attributes["title"].(string); ok {
			titles = append(titles, title)
		}
	}
	return titles, nil
}
