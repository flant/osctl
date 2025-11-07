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
	cfg := config.GetConfig()

	dataSourceName := cfg.GetDataSourceName()
	osdURL := cfg.GetOSDURL()

	osdURL = utils.NormalizeURL(osdURL)

	if dataSourceName == "" || cfg.GetOpenSearchURL() == "" || osdURL == "" {
		return fmt.Errorf("dataSourceName, os-url and osd-url parameters are required")
	}

	logger := logging.NewLogger()
	dryRun := cfg.GetDryRun()
	_, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	user := cfg.GetKibanaUser()
	pass := cfg.GetKibanaPass()
	kb := kibana.NewClient(osdURL, user, pass, cfg.GetTimeout())

	tenants := []string{"global"}
	tenantNamesForLog := []string{"global"}
	if cfg.GetDataSourceKibanaMultitenancy() {
		tf, err := config.GetConfig().GetTenantsConfig()
		if err != nil {
			return err
		}
		for _, t := range tf.Tenants {
			tenants = append(tenants, t.Name)
			tenantNamesForLog = append(tenantNamesForLog, t.Name)
		}
	}
	var createdDataSources []string
	var existingDataSources []string

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
				logger.Info(fmt.Sprintf("DRY RUN: Would create data source '%s' in tenant %s", dataSourceName, tenantNameForLog))
				createdDataSources = append(createdDataSources, fmt.Sprintf("%s (tenant=%s)", dataSourceName, tenantNameForLog))
			} else {
				if err := kb.CreateDataSource(tenant, dataSourceName, cfg.GetOpenSearchURL(), user, pass); err != nil {
					return err
				}
				logger.Info(fmt.Sprintf("Created data source '%s' in tenant %s", dataSourceName, tenantNameForLog))
				createdDataSources = append(createdDataSources, fmt.Sprintf("%s (tenant=%s)", dataSourceName, tenantNameForLog))
			}
		} else {
			logger.Info(fmt.Sprintf("Data source already exists in tenant %s (title=%s)", tenantNameForLog, dataSourceName))
			existingDataSources = append(existingDataSources, fmt.Sprintf("%s (tenant=%s)", dataSourceName, tenantNameForLog))
		}
	}

	if cfg.GetDataSourceKibanaMultidomainEnabled() {
		remote := cfg.GetDataSourceRemoteCRT()
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
		ns := cfg.GetKubeNamespace()

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
				return nil
			}
			existing.Data = map[string][]byte{"multi.crt": desired}
			if _, err := cs.CoreV1().Secrets(ns).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
				return err
			}
		} else if apierrors.IsNotFound(err) {
			if dryRun {
				logger.Info("DRY RUN: Would create secret multi-certs with multi.crt contents")
				return nil
			}
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "multi-certs", Namespace: ns}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"multi.crt": desired}}
			if _, err := cs.CoreV1().Secrets(ns).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("failed to check multi-certs secret: %w", err)
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

	if !dryRun {
		fmt.Println("\n" + strings.Repeat("=", 60))
		fmt.Println("DATA SOURCE SUMMARY")
		fmt.Println(strings.Repeat("=", 60))
		if len(createdDataSources) > 0 {
			fmt.Printf("Created: %d data sources\n", len(createdDataSources))
			for _, name := range createdDataSources {
				fmt.Printf("  ✓ %s\n", name)
			}
		}
		if len(existingDataSources) > 0 {
			fmt.Printf("\nAlready exists: %d data sources\n", len(existingDataSources))
			for _, name := range existingDataSources {
				fmt.Printf("  - %s\n", name)
			}
		}
		if len(createdDataSources) == 0 && len(existingDataSources) == 0 {
			fmt.Println("No data sources were added")
		}
		fmt.Println(strings.Repeat("=", 60))
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
