package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"rokid-prism-hub/internal/agentlocation"
	"rokid-prism-hub/internal/pluginruntime"
)

// 前台服务（例如 opencode serve）必须在启动请求结束后继续运行。
// 这里只跟踪并停止本次启动的子进程；已有用户服务只做探测，不接管也不终止。
type managedAgentService struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error // published by closing done
	output serviceOutput
}
type serviceOutput struct {
	sync.Mutex
	tail []byte
}

func (o *serviceOutput) Write(data []byte) (int, error) {
	o.Lock()
	defer o.Unlock()
	o.tail = append(o.tail, data...)
	if len(o.tail) > 4096 {
		o.tail = append([]byte(nil), o.tail[len(o.tail)-4096:]...)
	}
	return len(data), nil
}
func (o *serviceOutput) detail() string {
	o.Lock()
	defer o.Unlock()
	return strings.TrimSpace(string(o.tail))
}

var managedAgentServices = struct {
	sync.Mutex
	items map[string]*managedAgentService
}{items: map[string]*managedAgentService{}}

func startTrackedAgentService(id string, cmd *exec.Cmd) (*managedAgentService, bool, error) {
	managedAgentServices.Lock()
	defer managedAgentServices.Unlock()
	if existing := managedAgentServices.items[id]; existing != nil {
		return existing, false, nil
	}
	running := &managedAgentService{cmd: cmd, done: make(chan struct{})}
	cmd.Stdout = &running.output
	cmd.Stderr = &running.output
	agentlocation.PrepareService(cmd)
	if err := cmd.Start(); err != nil {
		return nil, false, err
	}
	managedAgentServices.items[id] = running
	go func() {
		running.err = cmd.Wait()
		managedAgentServices.Lock()
		if managedAgentServices.items[id] == running {
			delete(managedAgentServices.items, id)
		}
		managedAgentServices.Unlock()
		close(running.done)
	}()
	return running, true, nil
}
func stopManagedAgentServices() {
	managedAgentServices.Lock()
	services := make([]*managedAgentService, 0, len(managedAgentServices.items))
	for _, service := range managedAgentServices.items {
		services = append(services, service)
	}
	managedAgentServices.Unlock()
	for _, service := range services {
		select {
		case <-service.done:
			continue
		default:
		}
		agentlocation.StopService(service.cmd, service.done)
		select {
		case <-service.done:
		case <-time.After(3 * time.Second):
		}
	}
}
func probeManagedService(ctx context.Context, svc *pluginruntime.Service, id string) (bool, string) {
	if svc == nil {
		return false, "Plugin runtime is unavailable"
	}
	adapter := svc.Adapter(id)
	if adapter == nil {
		return false, "Plugin runtime is unavailable"
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	capability, err := adapter.Probe(probeCtx)
	if err != nil {
		return false, err.Error()
	}
	return capability.Available, capability.UnavailableReason
}
func waitManagedAgentService(ctx context.Context, svc *pluginruntime.Service, id string, running *managedAgentService, created bool) error {
	for {
		if available, _ := probeManagedService(ctx, svc, id); available {
			return nil
		}
		select {
		case <-running.done:
			if running.err != nil {
				return fmt.Errorf("Agent service failed: %v: %s", running.err, running.output.detail())
			}
			// 成功转为后台运行的启动器可能在服务就绪前退出。
		default:
		}
		select {
		case <-ctx.Done():
			if created {
				select {
				case <-running.done:
				default:
					agentlocation.StopService(running.cmd, running.done)
				}
			}
			return fmt.Errorf("Agent service did not become available: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}
