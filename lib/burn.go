package trunks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

var (
	ErrNoSubConn   = fmt.Errorf("No Sub Connections")
	ErrNoGrpcHosts = fmt.Errorf("No gRPC Hosts Provided")
)

// simple client-side round-robin pool
type pool struct {
	conns []*grpc.ClientConn

	mu   sync.Mutex
	next int
}

func (p *pool) Pick() (*grpc.ClientConn, error) {
	size := len(p.conns)
	if size <= 0 {
		return nil, ErrNoSubConn
	}

	p.mu.Lock()
	defer func() {
		p.mu.Unlock()
		p.next = (p.next + 1) % size
	}()
	return p.conns[p.next], nil
}

func (p *pool) Close() error {
	var errs []string
	for _, c := range p.conns {
		if err := c.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, ", "))
	}
	return nil
}

// which gRPC method to call
type Gtarget struct {
	MethodName string
	Request    proto.Message
	Response   proto.Message
}

// the burner
type Burner struct {
	pool      *pool
	numWorker uint64
	ctx       context.Context
	stopch    chan struct{}
}

func NewBurner(hosts []string, opts ...func(*Burner)) (*Burner, error) {
	if hosts == nil || len(hosts) <= 0 {
		return nil, ErrNoGrpcHosts
	}

	p := &pool{}
	for _, h := range hosts {
		c, err := grpc.Dial(h, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("Init pool failed: %v", err)
		}
		p.conns = append(p.conns, c)
	}

	b := &Burner{
		pool:      p,
		stopch:    make(chan struct{}),
		numWorker: DefaultWorkers,
		ctx:       context.Background(),
	}

	for _, opt := range opts {
		opt(b)
	}

	return b, nil
}

func NumWorker(num uint64) func(*Burner) {
	return func(b *Burner) { b.numWorker = num }
}

func (b *Burner) Close() error {
	return b.pool.Close()
}

func (b *Burner) Burn(tgt *Gtarget, rate uint64, du time.Duration) <-chan *Result {
	var workers sync.WaitGroup
	results := make(chan *Result)
	ticks := make(chan time.Time)
	for i := uint64(0); i < b.numWorker; i++ {
		workers.Add(1)
		go b.burn(tgt, &workers, ticks, results)
	}

	go func() {
		defer close(results)
		defer workers.Wait()
		defer close(ticks)
		interval := 1e9 / rate
		hits := rate * uint64(du.Seconds())
		began, done := time.Now(), uint64(0)
		for {
			now, next := time.Now(), began.Add(time.Duration(done*interval))
			time.Sleep(next.Sub(now))
			select {
			case ticks <- max(next, now):
				if done++; done == hits {
					return
				}
			case <-b.stopch:
				return
			default: // all workers are blocked. start one more and try again
				workers.Add(1)
				go b.burn(tgt, &workers, ticks, results)
			}
		}
	}()

	return results
}

func (b *Burner) Stop() {
	select {
	case <-b.stopch:
		return
	default:
		close(b.stopch)
	}
}

func (b *Burner) burn(tgt *Gtarget, workers *sync.WaitGroup, ticks <-chan time.Time, results chan<- *Result) {
	defer workers.Done()
	for tm := range ticks {
		results <- b.hit(tgt, tm)
	}
}

func (b *Burner) hit(tgt *Gtarget, tm time.Time) *Result {
	var res = Result{Timestamp: tm}
	var err error

	defer func() {
		res.Latency = time.Since(tm)
		if err != nil {
			res.Error = err.Error()
		}
	}()

	c, err := b.pool.Pick()
	if err != nil {
		b.Stop()
		return &res
	}

	// TODO: add gRPC CallOptions
	err = c.Invoke(b.ctx, tgt.MethodName, tgt.Request, tgt.Response)
	return &res
}
