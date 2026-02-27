package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClaimWriter writes GPUNodeClaim lifecycle events to Postgres.
// It is wired into the reconciler and disruptor to record phase transitions.
// If pool is nil all writes are no-ops so the controller works without Postgres.
type ClaimWriter struct {
	pool *pgxpool.Pool
}

func NewClaimWriter(postgresURL string) (*ClaimWriter, error) {
	if postgresURL == "" {
		return &ClaimWriter{}, nil
	}
	cfg, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres URL: %w", err)
	}
	cfg.MaxConns = 4
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &ClaimWriter{pool: pool}, nil
}

func (w *ClaimWriter) Close() {
	if w.pool != nil {
		w.pool.Close()
	}
}

// ClaimWriteRecord holds the fields the controller knows at write time.
type ClaimWriteRecord struct {
	Name          string
	Pool          string
	Provider      string
	GPUType       string
	GPUCount      int
	PricePerHour  float64
	ModelID       string
	NodeType      string
	Phase         string
	ProvisionedAt *time.Time
	ReadyAt       *time.Time
}

// Upsert inserts or updates a claim record.
// Terminated_at and lifetime are set automatically the first time phase=Terminated is written.
func (w *ClaimWriter) Upsert(ctx context.Context, r ClaimWriteRecord) error {
	if w.pool == nil {
		return nil
	}

	var bootstrapS *int
	if r.ProvisionedAt != nil && r.ReadyAt != nil {
		d := int(r.ReadyAt.Sub(*r.ProvisionedAt).Seconds())
		bootstrapS = &d
	}

	_, err := w.pool.Exec(ctx, `
		INSERT INTO gpu_node_claims (
			name, pool, provider, gpu_type, gpu_count, price_per_hour,
			model_id, node_type, phase,
			provisioned_at, ready_at,
			bootstrap_duration_s,
			updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9,
			$10, $11,
			$12,
			NOW()
		)
		ON CONFLICT (name) DO UPDATE SET
			pool          = COALESCE(NULLIF(EXCLUDED.pool, ''),     gpu_node_claims.pool),
			provider      = COALESCE(NULLIF(EXCLUDED.provider, ''), gpu_node_claims.provider),
			gpu_type      = COALESCE(NULLIF(EXCLUDED.gpu_type, ''), gpu_node_claims.gpu_type),
			gpu_count     = CASE WHEN EXCLUDED.gpu_count > 0 THEN EXCLUDED.gpu_count ELSE gpu_node_claims.gpu_count END,
			price_per_hour = CASE WHEN EXCLUDED.price_per_hour > 0 THEN EXCLUDED.price_per_hour ELSE gpu_node_claims.price_per_hour END,
			model_id      = COALESCE(NULLIF(EXCLUDED.model_id, ''), gpu_node_claims.model_id),
			node_type     = COALESCE(NULLIF(EXCLUDED.node_type, ''), gpu_node_claims.node_type),
			phase         = EXCLUDED.phase,
			provisioned_at = COALESCE(gpu_node_claims.provisioned_at, EXCLUDED.provisioned_at),
			ready_at      = COALESCE(gpu_node_claims.ready_at, EXCLUDED.ready_at),
			bootstrap_duration_s = COALESCE(gpu_node_claims.bootstrap_duration_s, EXCLUDED.bootstrap_duration_s),
			terminated_at = CASE
				WHEN EXCLUDED.phase = 'Terminated' AND gpu_node_claims.terminated_at IS NULL THEN NOW()
				ELSE gpu_node_claims.terminated_at
			END,
			lifetime_s = CASE
				WHEN EXCLUDED.phase = 'Terminated' AND gpu_node_claims.lifetime_s IS NULL
				THEN EXTRACT(EPOCH FROM (NOW() - COALESCE(gpu_node_claims.provisioned_at, EXCLUDED.provisioned_at)))::INT
				ELSE gpu_node_claims.lifetime_s
			END,
			estimated_cost = CASE
				WHEN EXCLUDED.phase = 'Terminated' AND gpu_node_claims.estimated_cost IS NULL
				THEN ROUND(
					COALESCE(gpu_node_claims.price_per_hour, EXCLUDED.price_per_hour) *
					EXTRACT(EPOCH FROM (NOW() - COALESCE(gpu_node_claims.provisioned_at, EXCLUDED.provisioned_at))) / 3600,
					4)
				ELSE gpu_node_claims.estimated_cost
			END,
			updated_at    = NOW()
	`,
		r.Name, r.Pool, r.Provider, r.GPUType, r.GPUCount, r.PricePerHour,
		r.ModelID, r.NodeType, r.Phase,
		r.ProvisionedAt, r.ReadyAt,
		bootstrapS,
	)
	return err
}

// WriteEvent inserts a bootstrap event for a claim. No-op if pool is nil.
func (cw *ClaimWriter) WriteEvent(ctx context.Context, claimName, step, message string) error {
	if cw.pool == nil {
		return nil
	}
	_, err := cw.pool.Exec(ctx,
		`INSERT INTO bootstrap_events (claim_name, step, message) VALUES ($1, $2, $3)`,
		claimName, step, message,
	)
	return err
}
