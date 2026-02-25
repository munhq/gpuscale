package controller

import (
	"context"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// InstanceGC periodically lists all provider instances and destroys any
// that don't have a matching GPUNodeClaim. This catches orphans from
// controller restarts, failed finalizers, or force-deleted claims.
//
// To avoid a race between instance creation and annotation/status propagation
// in the informer cache, an instance must appear orphaned in TWO consecutive
// sweeps before it is destroyed. This gives the reconciler at least one full
// GC interval to write the claim annotation.
type InstanceGC struct {
	client.Client
	Log      logr.Logger
	Registry *provider.Registry
	Interval time.Duration

	// candidates tracks instanceIDs seen as orphaned in the previous sweep.
	// Only instances present in both the previous and current sweep are destroyed.
	candidates map[string]bool // instanceID → true
}

func NewInstanceGC(c client.Client, log logr.Logger, registry *provider.Registry, interval time.Duration) *InstanceGC {
	return &InstanceGC{
		Client:     c,
		Log:        log,
		Registry:   registry,
		Interval:   interval,
		candidates: make(map[string]bool),
	}
}

// Start implements manager.Runnable. Runs an initial sweep then ticks every Interval.
func (g *InstanceGC) Start(ctx context.Context) error {
	log := g.Log
	log.Info("Starting instance garbage collector", "interval", g.Interval)

	// Initial sweep after a delay to let the informer cache populate.
	// Two minutes is enough for the reconciler to process all existing claims
	// and write their instance-id annotations after a controller restart.
	time.Sleep(2 * time.Minute)
	g.sweep(ctx)

	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Instance GC shutting down")
			return nil
		case <-ticker.C:
			g.sweep(ctx)
		}
	}
}

func (g *InstanceGC) sweep(ctx context.Context) {
	log := g.Log

	// Build set of known instance IDs from all non-Terminated claims.
	var claims v1alpha1.GPUNodeClaimList
	if err := g.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		log.Error(err, "GC: failed to list claims")
		return
	}

	knownInstances := make(map[string]string) // instanceID → claimName
	for _, c := range claims.Items {
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated {
			continue
		}
		if id := c.Status.InstanceID; id != "" && id != "0" {
			knownInstances[id] = c.Name
		}
		// Also check annotation (set before status update, more reliable).
		if id := c.Annotations["gpuscale.io/instance-id"]; id != "" && id != "0" {
			knownInstances[id] = c.Name
		}
	}

	// Collect the set of orphaned instance IDs seen in this sweep.
	currentOrphans := make(map[string]bool)

	for _, prov := range g.Registry.List() {
		instances, err := prov.ListInstances(ctx)
		if err != nil {
			log.Error(err, "GC: failed to list instances", "provider", prov.Name())
			continue
		}

		for _, inst := range instances {
			if _, claimed := knownInstances[inst.InstanceID]; claimed {
				continue
			}

			// Instance not claimed. Check if it was also orphaned last sweep.
			if !g.candidates[inst.InstanceID] {
				// First time we see this as orphaned — record and wait one more cycle.
				log.Info("GC: orphan candidate (will destroy next sweep if still unclaimed)",
					"provider", prov.Name(),
					"instanceID", inst.InstanceID,
					"gpu", inst.GPUType,
					"status", inst.Status,
				)
				currentOrphans[inst.InstanceID] = true
				continue
			}

			// Orphaned in both previous and current sweep — safe to destroy.
			log.Info("GC: destroying orphaned instance",
				"provider", prov.Name(),
				"instanceID", inst.InstanceID,
				"gpu", inst.GPUType,
				"status", inst.Status,
			)
			if err := prov.DestroyInstance(ctx, inst.InstanceID); err != nil {
				log.Error(err, "GC: failed to destroy orphaned instance",
					"provider", prov.Name(),
					"instanceID", inst.InstanceID,
				)
				// Keep in candidates so we retry next sweep.
				currentOrphans[inst.InstanceID] = true
			} else {
				log.Info("GC: orphaned instance destroyed",
					"provider", prov.Name(),
					"instanceID", inst.InstanceID,
				)
			}
		}
	}

	// Advance the candidate set: only instances seen as orphaned this sweep
	// survive into the next sweep's candidate set.
	g.candidates = currentOrphans
}

// SetupWithManager registers the GC as a Runnable.
func (g *InstanceGC) SetupWithManager(mgr ctrl.Manager) error {
	return mgr.Add(g)
}

// NeedLeaderElection returns true so the GC only runs on the leader.
func (g *InstanceGC) NeedLeaderElection() bool {
	return true
}
