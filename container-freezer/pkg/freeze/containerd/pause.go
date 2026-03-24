package containerd

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	"google.golang.org/grpc"

	"knative.dev/container-freezer/pkg/freeze/common"
)

const defaultContainerdAddress = "/var/run/containerd/containerd.sock"

// NewContainerdProvider returns a CRI based on Containerd
func NewContainerdProvider() (*ContainerdCRI, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	address := os.Getenv("CONTAINERD_ADDRESS")
	if address == "" {
		address = defaultContainerdAddress
	}

	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*16)), grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", addr)
	}))
	if err != nil {
		return nil, err
	}

	client, err := containerd.NewWithConn(conn)
	if err != nil {
		return nil, err
	}

	return &ContainerdCRI{conn: conn, ctrd: client}, nil
}

type ContainerdCRI struct {
	conn *grpc.ClientConn
	ctrd *containerd.Client
}

// List returns a list of all non queue-proxy container IDs in a given pod.
func (c *ContainerdCRI) List(ctx context.Context, podKey string) ([]string, error) {
	return common.List(ctx, c.conn, podKey)
}

// killAllPids sends sig to every PID listed in a containerd task.
// It uses task.Pids() to enumerate all processes in the container (including
// child processes like gunicorn workers) and kills them directly via
// syscall.Kill. This avoids both the cgroup freezer subsystem
// (CONFIG_CGROUP_FREEZER absent on NXP 5.4.70+ kernels) and the runc
// kill --all path which relies on cgroup freezer for atomicity.
// Requires the daemon pod to run with hostPID: true.
func killAllPids(ctx context.Context, task containerd.Task, sig syscall.Signal) error {
	pids, err := task.Pids(ctx)
	if err != nil {
		return fmt.Errorf("listing pids: %v", err)
	}
	var lastErr error
	for _, info := range pids {
		if err := syscall.Kill(int(info.Pid), sig); err != nil && err != syscall.ESRCH {
			lastErr = err
		}
	}
	return lastErr
}

// Pause suspends a container by sending SIGSTOP to all its processes.
func (c *ContainerdCRI) Pause(ctx context.Context, containerID string) error {
	nctx := namespaces.WithNamespace(ctx, "k8s.io")
	ctr, err := c.ctrd.LoadContainer(nctx, containerID)
	if err != nil {
		return fmt.Errorf("%s not paused: %v", containerID, err)
	}
	task, err := ctr.Task(nctx, nil)
	if err != nil {
		return fmt.Errorf("%s not paused: %v", containerID, err)
	}
	if err := killAllPids(nctx, task, syscall.SIGSTOP); err != nil {
		return fmt.Errorf("%s not paused: %v", containerID, err)
	}
	return nil
}

// Resume un-suspends a container by sending SIGCONT to all its processes.
func (c *ContainerdCRI) Resume(ctx context.Context, containerID string) error {
	nctx := namespaces.WithNamespace(ctx, "k8s.io")
	ctr, err := c.ctrd.LoadContainer(nctx, containerID)
	if err != nil {
		return fmt.Errorf("%s not resumed: %v", containerID, err)
	}
	task, err := ctr.Task(nctx, nil)
	if err != nil {
		return fmt.Errorf("%s not resumed: %v", containerID, err)
	}
	if err := killAllPids(nctx, task, syscall.SIGCONT); err != nil {
		return fmt.Errorf("%s not resumed: %v", containerID, err)
	}
	return nil
}
