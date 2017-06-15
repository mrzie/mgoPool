package mgoPool

import (
	"sync"
	"time"

	"gopkg.in/mgo.v2"
)

// 考虑未来可能同时连接多个数据库
// 将连接池单独做成一个struct
type Pool struct {
	mux          sync.Mutex
	min          uint
	max          uint
	count        uint
	inuse        uint
	resources    chan *mgo.Session
	gcBlocker    chan bool
	gcMux        sync.Mutex
	gcProcessing bool
	avaliable    bool
	db_url       string
}

type mgoPoolException struct {
	msg string
}

func (e mgoPoolException) Error() string {
	return e.msg
}

func Err(s string) mgoPoolException {
	return mgoPoolException{msg: s}
}

const (
	defaultTimeout = time.Second * 5
	gcCycle        = time.Minute * 5 // 每隔五分钟进行一次gc
)

func (p *Pool) waitingGC() {
	select {
	case <-time.After(gcCycle):
		p.mux.Lock()
		defer p.mux.Unlock()
		p.gcMux.Lock()
		defer p.gcMux.Unlock()
		var num uint

		if p.inuse > p.min {
			num = p.count - p.inuse
			defer func() {
				go p.waitingGC()
			}()
		} else {
			num = p.count - p.min
			// 当连接数达到最小值，中止gc循环
			defer func() {
				p.gcProcessing = false
			}()
		}

		p.count -= num

		for ; num > 0; num-- {
			session := <-p.resources
			session.Close()
		}

		return
	case <-p.gcBlocker:
		p.gcMux.Lock()
		p.gcProcessing = false
		p.gcMux.Unlock()
		return
	}
}

func (p *Pool) startWaitingGC() {
	p.gcMux.Lock()
	if p.gcProcessing {
		p.gcBlocker <- true
	}
	p.gcMux.Unlock()

	go func() {
		p.gcMux.Lock()
		p.gcProcessing = true
		p.gcMux.Unlock()
		p.waitingGC()
	}()
}

func (p *Pool) create() (*mgo.Session, error) {
	return mgo.DialWithTimeout(p.db_url, defaultTimeout)
}

// 默认的超时时间为五秒
func (p *Pool) Acquire() (*mgo.Session, error) {
	if !p.avaliable {
		return nil, Err("pool invalid")
	}

	p.mux.Lock()
	// 当前链接满载的情况下，试图创建一个新的连接
	if p.inuse >= p.count && p.count < p.max {
		defer p.mux.Unlock()
		session, err := p.create()
		if err != nil {
			return nil, err
		}

		p.count++
		p.inuse++

		// 每当新增一个连接时，中止当前gc循环并重新开始一个gc循环
		p.startWaitingGC()

		return session, nil
	}
	p.inuse++
	p.mux.Unlock()

	// 非满载情况下，获取连接
	// 达到最大负载情况下，等待释放的连接
	select {
	case <-time.After(defaultTimeout):
		p.mux.Lock()
		p.inuse--
		p.mux.Unlock()
		return nil, Err("db timeout")
	case session := <-p.resources:
		return session, nil
	}
}

func (p *Pool) Release(s *mgo.Session) {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.inuse--
	p.resources <- s
}

func CreatePool(url string, min uint, max uint) (p *Pool, err error) {
	p = new(Pool)
	p.db_url = url

	if min > max {
		min = max
	}
	p.min = min
	p.max = max

	p.resources = make(chan *mgo.Session, max)
	p.gcBlocker = make(chan bool)

	var conn *mgo.Session
	for ; min > 0; min-- {
		conn, err = p.create()
		if err != nil {
			return
		}
		p.resources <- conn
		p.count++
	}
	p.avaliable = true
	return
}

func (p *Pool) Drain() {
	// 排干连接池
	p.avaliable = false
	for p.count > 0 {
		session := <-p.resources
		session.Close()
	}
	close(p.resources)
	close(p.gcBlocker)
}

func (p *Pool) Abandon(s *mgo.Session) {
	p.mux.Lock()
	defer p.mux.Unlock()
	s.Close()
	p.inuse--
	p.count--
}
