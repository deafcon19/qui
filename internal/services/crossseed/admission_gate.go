package crossseed

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"

	"github.com/autobrr/qui/internal/models"
)

type admissionHeldKey struct{}
type admissionWorkerKey struct{}

type admissionScope []string

// acquireAdmission holds the external route lock from candidate discovery through
// the qBittorrent add. When WIP owns the lock, discovery waits until its final
// links exist, so all save-path decisions use the completed placement.
func (s *Service) acquireAdmission(ctx context.Context, instanceIDs []int) (context.Context, func(), bool, error) {
	command := strings.TrimSpace(os.Getenv("QUI_CROSS_SEED_GATE_COMMAND"))
	if command == "" {
		return ctx, func() {}, true, nil
	}
	configured := strings.TrimSpace(os.Getenv("QUI_CROSS_SEED_GATE_INSTANCES"))
	if configured == "" {
		return ctx, nil, false, errors.New("QUI_CROSS_SEED_GATE_INSTANCES is required when the gate command is configured")
	}
	// Fresh arrivals join the durable queue instead of holding earlier routes
	// while probing every other route ahead of older target-specific work.
	if ctx.Value(admissionWorkerKey{}) == nil && ctx.Value(admissionHeldKey{}) == nil {
		return ctx, func() {}, false, nil
	}
	instances, err := s.admissionInstances(ctx, instanceIDs)
	if err != nil {
		return ctx, nil, false, err
	}
	names := admissionRouteNames(configured, instances)
	if held, ok := ctx.Value(admissionHeldKey{}).(admissionScope); ok {
		for _, name := range names {
			if !slices.Contains(held, name) {
				return ctx, nil, false, errors.New("cross-seed admission route changed; retry with a fresh lease")
			}
		}
		return ctx, func() {}, true, nil
	}
	type heldCommand struct {
		cmd   *exec.Cmd
		stdin io.WriteCloser
	}
	held := make([]heldCommand, 0, len(names))
	release := func() {
		for _, h := range slices.Backward(held) {
			_ = h.stdin.Close()
			_ = h.cmd.Wait()
		}
	}
	for _, name := range names {
		// The executable is operator-controlled, and name is one argv element.
		cmd := exec.CommandContext(ctx, command, name) //nolint:gosec // trusted configured executable
		stdin, err := cmd.StdinPipe()
		if err != nil {
			release()
			return ctx, nil, false, fmt.Errorf("open cross-seed gate input: %w", err)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			release()
			return ctx, nil, false, fmt.Errorf("open cross-seed gate output: %w", err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			release()
			return ctx, nil, false, fmt.Errorf("start cross-seed gate for %s: %w", name, err)
		}
		held = append(held, heldCommand{cmd: cmd, stdin: stdin})
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			release()
			return ctx, nil, false, fmt.Errorf("cross-seed gate for %s did not become ready: %w", name, err)
		}
		if strings.TrimSpace(line) == "WAIT" {
			release()
			return ctx, func() {}, false, nil
		}
		if strings.TrimSpace(line) != "READY" {
			release()
			return ctx, nil, false, fmt.Errorf("cross-seed gate for %s returned %q", name, strings.TrimSpace(line))
		}
	}
	return context.WithValue(ctx, admissionHeldKey{}, admissionScope(names)), release, true, nil
}

func (s *Service) admissionInstances(ctx context.Context, instanceIDs []int) ([]*models.Instance, error) {
	instances := make([]*models.Instance, 0)
	if len(instanceIDs) == 0 {
		all, err := s.instanceStore.List(ctx)
		if err != nil {
			return nil, fmt.Errorf("list instances for cross-seed gate: %w", err)
		}
		instances = all
	} else {
		for _, id := range normalizeInstanceIDs(instanceIDs) {
			instance, err := s.instanceStore.Get(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("resolve instance %d for cross-seed gate: %w", id, err)
			}
			instances = append(instances, instance)
		}
	}
	// A routed add also mutates its receiver, even when only the source was requested.
	for _, instance := range slices.Clone(instances) {
		if instance == nil {
			continue
		}
		route, _, err := s.savedRouteOf(ctx, instance.ID)
		if err != nil {
			return nil, fmt.Errorf("resolve admission receiver for %d: %w", instance.ID, err)
		}
		if route != nil && route.crossSeed != nil {
			instances = append(instances, route.crossSeed)
		}
	}
	return instances, nil
}

func admissionRouteNames(configured string, instances []*models.Instance) []string {
	guarded := make(map[string]string)
	for entry := range strings.SplitSeq(configured, ",") {
		instanceName, routeName, hasRoute := strings.Cut(strings.TrimSpace(entry), ":")
		if instanceName != "" {
			if !hasRoute {
				routeName = instanceName
			}
			guarded[instanceName] = routeName
		}
	}
	names := make([]string, 0)
	for _, instance := range instances {
		if instance == nil {
			continue
		}
		if routeName, ok := guarded[instance.Name]; ok {
			names = append(names, routeName)
		}
	}
	sort.Strings(names)
	return slices.Compact(names)
}
