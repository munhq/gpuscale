package metrics

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func claimNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "gpu-workloads"
}

// WorkerMetricsCollector scrapes /metrics from Ready GPUNodeClaim endpoints
// and re-exposes them with provider/instance labels.
type WorkerMetricsCollector struct {
	client     client.Client
	httpClient *http.Client
	interval   time.Duration

	mu      sync.RWMutex
	metrics map[string]*workerSnapshot // endpoint -> latest scrape
}

type workerSnapshot struct {
	endpoint   string
	provider   string
	instanceID string
	gpuType    string
	scrapedAt  time.Time
	families   map[string]*dto.MetricFamily
}

// vLLM metrics we care about
var vllmMetrics = map[string]bool{
	"vllm:num_requests_running":                true,
	"vllm:num_requests_waiting":                true,
	"vllm:gpu_cache_usage_perc":                true,
	"vllm:cpu_cache_usage_perc":                true,
	"vllm:avg_generation_throughput_toks_per_s": true,
	"vllm:avg_prompt_throughput_toks_per_s":     true,
	"vllm:num_preemptions_total":               true,
	"vllm:request_success_total":               true,
	"vllm:request_failure_total":               true,
	"vllm:e2e_request_latency_seconds":         true,
	"vllm:time_to_first_token_seconds":         true,
}

// Prometheus descriptors for our own meta-metrics
var (
	workerScrapeSuccessDesc = prometheus.NewDesc(
		"gpuscale_worker_scrape_success",
		"Whether the last scrape of a worker endpoint succeeded (1=success, 0=failure)",
		[]string{"endpoint", "provider", "instance_id", "gpu_type"}, nil,
	)
	workerScrapeTimestampDesc = prometheus.NewDesc(
		"gpuscale_worker_scrape_timestamp_seconds",
		"Unix timestamp of the last successful scrape",
		[]string{"endpoint", "provider", "instance_id", "gpu_type"}, nil,
	)
	workerCountDesc = prometheus.NewDesc(
		"gpuscale_workers_total",
		"Total number of Ready workers being monitored",
		nil, nil,
	)
)

// NewWorkerMetricsCollector creates a new collector.
func NewWorkerMetricsCollector(c client.Client, interval time.Duration) *WorkerMetricsCollector {
	return &WorkerMetricsCollector{
		client: c,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		interval: interval,
		metrics:  make(map[string]*workerSnapshot),
	}
}

// Start begins the background scraping loop. Satisfies manager.Runnable.
func (c *WorkerMetricsCollector) Start(ctx context.Context) error {
	log.Printf("worker metrics collector started (interval=%s)", c.interval)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Initial scrape
	c.scrapeAll(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.scrapeAll(ctx)
		}
	}
}

func (c *WorkerMetricsCollector) scrapeAll(ctx context.Context) {
	var claims v1alpha1.GPUNodeClaimList
	if err := c.client.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		log.Printf("worker metrics: failed to list claims: %v", err)
		return
	}

	activeEndpoints := make(map[string]bool)

	for _, claim := range claims.Items {
		if claim.Status.Phase != v1alpha1.ClaimPhaseReady {
			continue
		}
		endpoint := claim.Status.Endpoint
		if endpoint == "" {
			continue
		}

		activeEndpoints[endpoint] = true
		c.scrapeWorker(ctx, endpoint, claim.Status.Provider, claim.Status.InstanceID, claim.Status.GPUType)
	}

	// Clean up stale entries
	c.mu.Lock()
	for ep := range c.metrics {
		if !activeEndpoints[ep] {
			delete(c.metrics, ep)
		}
	}
	c.mu.Unlock()
}

func (c *WorkerMetricsCollector) scrapeWorker(ctx context.Context, endpoint, providerName, instanceID, gpuType string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/metrics", nil)
	if err != nil {
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	// Parse Prometheus text format
	parser := &expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		log.Printf("worker metrics: failed to parse metrics from %s: %v", endpoint, err)
		return
	}

	// Filter to only vLLM metrics we care about
	filtered := make(map[string]*dto.MetricFamily)
	for name, fam := range families {
		if vllmMetrics[name] {
			filtered[name] = fam
		}
	}

	snap := &workerSnapshot{
		endpoint:   endpoint,
		provider:   providerName,
		instanceID: instanceID,
		gpuType:    gpuType,
		scrapedAt:  time.Now(),
		families:   filtered,
	}

	c.mu.Lock()
	c.metrics[endpoint] = snap
	c.mu.Unlock()
}

// Describe implements prometheus.Collector.
func (c *WorkerMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- workerScrapeSuccessDesc
	ch <- workerScrapeTimestampDesc
	ch <- workerCountDesc
}

// Collect implements prometheus.Collector.
// It re-emits scraped vLLM metrics with added labels.
func (c *WorkerMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ch <- prometheus.MustNewConstMetric(workerCountDesc, prometheus.GaugeValue, float64(len(c.metrics)))

	for _, snap := range c.metrics {
		labels := []string{snap.endpoint, snap.provider, snap.instanceID, snap.gpuType}

		ch <- prometheus.MustNewConstMetric(workerScrapeSuccessDesc, prometheus.GaugeValue, 1, labels...)
		ch <- prometheus.MustNewConstMetric(workerScrapeTimestampDesc, prometheus.GaugeValue, float64(snap.scrapedAt.Unix()), labels...)

		// Re-emit each vLLM metric with our labels prepended
		for name, fam := range snap.families {
			for _, m := range fam.GetMetric() {
				promName := fmt.Sprintf("gpuscale_worker_%s", strings.ReplaceAll(name, ":", "_"))

				// Build label names/values: our labels + original metric labels
				labelNames := []string{"endpoint", "provider", "instance_id", "gpu_type"}
				labelValues := []string{snap.endpoint, snap.provider, snap.instanceID, snap.gpuType}
				for _, lp := range m.GetLabel() {
					labelNames = append(labelNames, lp.GetName())
					labelValues = append(labelValues, lp.GetValue())
				}

				desc := prometheus.NewDesc(promName, fam.GetHelp(), labelNames, nil)

				switch fam.GetType() {
				case dto.MetricType_GAUGE:
					if g := m.GetGauge(); g != nil {
						ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, g.GetValue(), labelValues...)
					}
				case dto.MetricType_COUNTER:
					if ct := m.GetCounter(); ct != nil {
						ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, ct.GetValue(), labelValues...)
					}
				case dto.MetricType_HISTOGRAM:
					if h := m.GetHistogram(); h != nil {
						buckets := make(map[float64]uint64)
						for _, b := range h.GetBucket() {
							buckets[b.GetUpperBound()] = b.GetCumulativeCount()
						}
						ch <- prometheus.MustNewConstHistogram(desc, h.GetSampleCount(), h.GetSampleSum(), buckets, labelValues...)
					}
				}
			}
		}
	}
}
