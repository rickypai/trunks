package trunks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(codecIgnoreResp{})
}

var (
	ErrNoSubConn   = fmt.Errorf("No Sub Connections")
	ErrNoGrpcHosts = fmt.Errorf("No gRPC Hosts Provided")
	ErrNoRequest   = fmt.Errorf("No request provided")
)

// simple client-side round-robin pool
type pool struct {
	conns []*grpc.ClientConn
	mu    sync.Mutex
	next  int
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

// Gtarget represents the task/target of stress testing
type Gtarget struct {
	MethodName string
	Requests   []proto.Message
	Response   proto.Message // origin cell to be cloned

	indexMux sync.Mutex // mutex for index of requests
	index    uint
}

// If multiple requests given, pick up one
func (tgt *Gtarget) getRequest(loop bool) (proto.Message, error) {
	l := len(tgt.Requests)
	if l == 0 {
		return nil, ErrNoRequest
	}
	if l == 1 {
		return tgt.Requests[0], nil
	}

	tgt.indexMux.Lock()
	defer tgt.indexMux.Unlock()

	if !loop && tgt.index >= uint(l) {
		return nil, ErrNoRequest
	}

	_index := tgt.index % uint(l)
	req := tgt.Requests[_index]
	tgt.index++ // TODO: make sure it's OK even exceeding uint limitation
	return req, nil
}

// Burner is the subject to do the burning attack
type Burner struct {
	numWorker uint64
	loop      bool
	dump      bool
	dumpFile  string
	dialOpt   grpc.DialOption

	pool   *pool
	ctx    context.Context
	stopch chan struct{}
}

func NewBurner(hosts []string, opts ...func(*Burner)) (*Burner, error) {
	if hosts == nil || len(hosts) <= 0 {
		return nil, ErrNoGrpcHosts
	}

	b := &Burner{
		stopch:    make(chan struct{}),
		numWorker: DefaultWorkers,
		ctx:       context.Background(),
		dialOpt:   grpc.WithInsecure(),
	}

	for _, opt := range opts {
		opt(b)
	}

	b.initPool(hosts)

	return b, nil
}

// Deprecating; use WithNumWorker(uint64)
func NumWorker(num uint64) func(*Burner) {
	return func(b *Burner) { b.numWorker = num }
}

// Deprecating; use WithLooping(bool)
func WithLoop() func(*Burner) {
	return func(b *Burner) { b.loop = true }
}

func WithNumWorker(num uint64) func(*Burner) {
	return func(b *Burner) { b.numWorker = num }
}

func WithLooping(yesno bool) func(*Burner) {
	return func(b *Burner) { b.loop = yesno }
}

func WithDialOpt(dialOpt grpc.DialOption) func(*Burner) {
	return func(b *Burner) { b.dialOpt = dialOpt }
}

func WithDumpFile(fileName string) func(*Burner) {
	return func(b *Burner) {
		b.dumpFile = fileName
		if fileName != "" {
			b.dump = true
		}
	}
}

// WaitDumpDone waits until all response dumpings are done
// and write everything into the dump file
func (b *Burner) WaitDumpDone() error {
	// dummy-proof
	if !b.dump {
		return nil
	}

	dumpers.Wait()

	f, err := os.OpenFile(b.dumpFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(memBuf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (b *Burner) initPool(hosts []string) (*pool, error) {
	if b.pool != nil {
		return b.pool, nil
	}

	p := &pool{}
	for _, h := range hosts {
		c, err := grpc.Dial(h, b.dialOpt)
		if err != nil {
			return nil, fmt.Errorf("Dialing to [%s] failed: %v", h, err)
		}
		p.conns = append(p.conns, c)
	}
	b.pool = p

	return b.pool, nil
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
			default:
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

func (b *Burner) hit(tgt *Gtarget, tm time.Time) (res *Result) {
	res = &Result{Timestamp: tm}
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}

		res.Latency = time.Since(tm)

		// count failure by cheating metrics
		// as if this is an HTTP call
		if err != nil {
			if res.Code < 100 {
				// if res.code is not set
				// consider it the fault from caller side
				res.Code = 400
			}
			res.Error = err.Error()
		} else {
			res.Code = 200
		}
	}()

	c, err := b.pool.Pick()
	if err != nil {
		b.Stop()
		return
	}

	req, err := tgt.getRequest(b.loop)
	if err != nil {
		b.Stop()
		return
	}

	if !b.dump {
		// simply discard the response, no operation on that object
		// so it's able to share one single response object
		err = c.Invoke(b.ctx, tgt.MethodName, req, tgt.Response, grpc.CallContentSubtype("proto-ignore-resp"))
	} else {
		// clone a response object to avoid race-condition
		bb := reflect.New(reflect.TypeOf(tgt.Response).Elem()).Interface().(proto.Message)
		err = c.Invoke(b.ctx, tgt.MethodName, req, bb)
		if err == nil {
			// start a new routine to serialize and dump
			// to avoid blocking processing
			dumpers.Add(1)
			go func(resp proto.Message) {
				defer dumpers.Done()
				bytes, err := json.Marshal(resp)
				if err == nil {
					memBufMutex.Lock()
					defer memBufMutex.Unlock()
					bytes = append(bytes, []byte{'\r', '\n'}...)
					memBuf.Write(bytes)
				}
			}(bb)
		}
	}
	return
}
