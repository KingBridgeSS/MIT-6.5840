package raft

import (
	"sync"
	"sync/atomic"

	"6.5840/utils"
)

type Applier struct {
	applyCh *chan ApplyMsg
	q       utils.Queue[ApplyMsg]
	mu      sync.Mutex
	cond    *sync.Cond
	dead    int32
}

func MakeApplier(ch *chan ApplyMsg) *Applier {
	ap := Applier{}
	ap.applyCh = ch
	ap.cond = sync.NewCond(&ap.mu)
	ap.dead = 0
	go ap.committer()
	return &ap
}

func (ap *Applier) Apply(msg ApplyMsg) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	ap.q.Enqueue(msg)
	ap.cond.Signal()
}

func (ap *Applier) committer() {
	for !ap.killed() {
		ap.mu.Lock()
		for ap.q.IsEmpty() {
			ap.cond.Wait()
		}
		msg, ok := ap.q.Dequeue()
		ap.mu.Unlock()
		if !ok {
			continue
		}
		// fmt.Printf("send msg %+v", msg)
		*ap.applyCh <- msg
		// fmt.Println("done.")
	}
}
func (ap *Applier) Kill() {
	atomic.StoreInt32(&ap.dead, 1)
}

func (ap *Applier) killed() bool {
	z := atomic.LoadInt32(&ap.dead)
	return z == 1
}
