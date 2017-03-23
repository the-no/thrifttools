package thrifttools

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type Pool struct {
	Dial func() (interface{}, error)
	Eclose func(c interface{}) error
	TestOnBorrow func(c interface{}) error
	AutoPut      func(p *Pool, c interface{}) error
	MaxIdle int
	MaxActive int
	IdleTimeout time.Duration
	//waiting
	Wait    bool
	WaitNum int
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int
	idle list.List
}

var nowFun = time.Now
var (
	ErrPoolClosed    = errors.New("The Connection pool closed.")
	ErrPoolExhausted = errors.New("The connection pool exhausted")
)

type idleConn struct {
	c interface{}
	t time.Time
}

func NewPool(dialFn func() (interface{}, error), closeFn func(interface{}) error, maxIdle int) *Pool {
	return &Pool{Dial: dialFn, MaxIdle: maxIdle}
}

func NewDefaultPool(
	dialFn func() (interface{}, error),
	closeFn func(interface{}) error,
	heatbeatFn func(interface{}) error,
) *Pool {
	return &Pool{
		Dial:         dialFn,
		Eclose:       closeFn,
		TestOnBorrow: heatbeatFn,
		MaxActive:    20,
		MaxIdle:      3,
		IdleTimeout:  300 * time.Second,
		Wait:         true,
	}
}

func (p *Pool) Get() (interface{}, error) {
	c, err := p.get()
	return c, err
}

func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

func (p *Pool) WaitNums() int {
	p.mu.Lock()
	waitNum := p.WaitNum
	p.mu.Unlock()
	return waitNum
}
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		p.Eclose(e.Value.(idleConn).c)
	}
	return nil
}

func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

func (p *Pool) get() (interface{}, error) {
	p.mu.Lock()

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFun()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			p.Eclose(ic.c)
			p.mu.Lock()

		}
	}
	for {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c) == nil {
	
				return ic.c, nil
			}
			p.Eclose(ic.c)
			p.mu.Lock()
			p.release()
		}
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("get on closed pool")
		}

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()

			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			} else if p.AutoPut != nil {
				err = p.AutoPut(p, c)
				if err != nil {
					p.put(c, true)
					c = nil
				}
			}
			return c, err

		}
		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}
		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}
func (p *Pool) Put(c interface{}) error {
	return p.put(c, false)
}
func (p *Pool) put(c interface{}, forceClose bool) error {
	p.mu.Lock()

	if !p.closed && !forceClose {
		p.idle.PushFront(idleConn{t: nowFun(), c: c})
	
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}
	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}
	p.release()
	p.mu.Unlock()
	return p.Eclose(c)
}
