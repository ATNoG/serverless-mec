package freezer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	pi "knative.dev/security-guard/pkg/pluginterfaces"
)

const (
	plugName    = "freezer"
	plugVersion = "0.1.0"

	defaultIdleTimeout        = 30 * time.Second
	defaultFreezeCheckInterval = 5 * time.Second
	defaultReadyTimeout       = 10 * time.Second
	defaultReadyPollInterval  = 100 * time.Millisecond
)

type freezeRequest struct {
	Action    string `json:"action"`
	PodName   string `json:"podName"`
	Namespace string `json:"namespace"`
}

type freezerPlug struct {
	hostIP      string
	freezerPort string
	podName     string
	namespace   string
	userPort    string
	apiKey      string

	idleTimeout time.Duration

	mu          sync.Mutex
	frozen      bool
	lastRequest time.Time

	cancelFreezeLoop context.CancelFunc
}

func (p *freezerPlug) callFreezer(action string) error {
	body, err := json.Marshal(freezeRequest{
		Action:    action,
		PodName:   p.podName,
		Namespace: p.namespace,
	})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%s", p.hostIP, p.freezerPort)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("freezer daemon returned %d for action %s", resp.StatusCode, action)
	}
	return nil
}

// waitForAppReady polls the app container's TCP port until it accepts connections.
func (p *freezerPlug) waitForAppReady() error {
	addr := "localhost:" + p.userPort
	deadline := time.Now().Add(defaultReadyTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, defaultReadyPollInterval)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(defaultReadyPollInterval)
	}
	return fmt.Errorf("app on port %s not ready after %v", p.userPort, defaultReadyTimeout)
}

func (p *freezerPlug) ApproveRequest(req *http.Request) (*http.Request, error) {
	p.mu.Lock()
	p.lastRequest = time.Now()
	wasFrozen := p.frozen
	p.mu.Unlock()

	if wasFrozen {
		pi.Log.Infof("Freezer: thawing %s/%s before forwarding request", p.namespace, p.podName)
		if err := p.callFreezer("resume"); err != nil {
			pi.Log.Errorf("Freezer: thaw failed: %v", err)
			// continue anyway — don't drop the request
		} else {
			if err := p.waitForAppReady(); err != nil {
				pi.Log.Warnf("Freezer: %v", err)
			}
			p.mu.Lock()
			p.frozen = false
			p.mu.Unlock()
		}
	}

	return req, nil
}

func (p *freezerPlug) ApproveResponse(req *http.Request, resp *http.Response) (*http.Response, error) {
	return resp, nil
}

func (p *freezerPlug) Init(ctx context.Context, config map[string]string, serviceName string, namespace string, logger pi.Logger) context.Context {
	pi.Log.Infof("Freezer plug initializing for %s/%s", namespace, p.podName)

	freezeCtx, cancel := context.WithCancel(ctx)
	p.cancelFreezeLoop = cancel
	go p.freezeLoop(freezeCtx)

	return ctx
}

func (p *freezerPlug) freezeLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultFreezeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			idle := time.Since(p.lastRequest) > p.idleTimeout
			frozen := p.frozen
			p.mu.Unlock()

			if idle && !frozen {
				pi.Log.Infof("Freezer: %s/%s idle, freezing containers", p.namespace, p.podName)
				if err := p.callFreezer("pause"); err != nil {
					pi.Log.Errorf("Freezer: freeze failed: %v", err)
				} else {
					p.mu.Lock()
					p.frozen = true
					p.mu.Unlock()
				}
			}
		}
	}
}

func (p *freezerPlug) Shutdown() {
	if p.cancelFreezeLoop != nil {
		p.cancelFreezeLoop()
	}
	p.mu.Lock()
	frozen := p.frozen
	p.mu.Unlock()
	if frozen {
		if err := p.callFreezer("resume"); err != nil {
			pi.Log.Errorf("Freezer: resume on shutdown failed: %v", err)
		}
	}
	pi.Log.Infof("Freezer plug shutdown")
}

func (p *freezerPlug) PlugName() string    { return plugName }
func (p *freezerPlug) PlugVersion() string { return plugVersion }

func init() {
	hostIP := os.Getenv("HOST_IP")
	if hostIP == "" {
		pi.Log.Errorf("Freezer: HOST_IP not set, plug disabled")
		return
	}

	podName := os.Getenv("SERVING_POD")
	if podName == "" {
		pi.Log.Errorf("Freezer: SERVING_POD not set, plug disabled")
		return
	}

	namespace := os.Getenv("SERVING_NAMESPACE")
	if namespace == "" {
		pi.Log.Errorf("Freezer: SERVING_NAMESPACE not set, plug disabled")
		return
	}

	userPort := os.Getenv("USER_PORT")
	if userPort == "" {
		userPort = "8080"
	}

	freezerPort := os.Getenv("FREEZER_PORT")
	if freezerPort == "" {
		freezerPort = "9696"
	}

	idleTimeout := defaultIdleTimeout
	if v := os.Getenv("FREEZER_IDLE_TIMEOUT_SECONDS"); v != "" {
		var secs int
		if _, err := fmt.Sscanf(v, "%d", &secs); err == nil && secs > 0 {
			idleTimeout = time.Duration(secs) * time.Second
		}
	}

	p := &freezerPlug{
		hostIP:      hostIP,
		freezerPort: freezerPort,
		podName:     podName,
		namespace:   namespace,
		userPort:    userPort,
		apiKey:      os.Getenv("FREEZER_API_KEY"),
		idleTimeout: idleTimeout,
		lastRequest: time.Now(),
	}

	pi.RegisterPlug(p)
}
