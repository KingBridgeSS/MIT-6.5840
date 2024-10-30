package kvraft

import (
	"math/rand"

	"6.5840/labrpc"
)

type requester struct {
	leader  int
	servers []*labrpc.ClientEnd
}

func MakeRequester(servers []*labrpc.ClientEnd) *requester {
	rt := new(requester)
	rt.servers = servers
	rt.rollALeader()
	return rt
}
func (rt *requester) rollALeader() {
	rt.leader = rand.Intn(len(rt.servers))
}

// 策略模式
type Strategy interface {
	Excecute() (bool, Err)
}
type GetRequest struct {
	Rt    *requester
	Args  *GetArgs
	Reply *GetReply
}

func (r *GetRequest) Excecute() (bool, Err) {
	ok := r.Rt.servers[r.Rt.leader].Call("KVServer.Get", r.Args, r.Reply)
	return ok, r.Reply.Err
}

type PutAppendRequest struct {
	Rt    *requester
	Op    string
	Args  *PutAppendArgs
	Reply *PutAppendReply
}

func (r *PutAppendRequest) Excecute() (bool, Err) {
	ok := r.Rt.servers[r.Rt.leader].Call("KVServer."+r.Op, r.Args, r.Reply)
	return ok, r.Reply.Err
}

type Context struct {
	Strategy Strategy
	Rt       *requester
	// GetRequest       *GetRequest
	// PutAppendRequest *PutAppendRequest
}

func (c *Context) process() string {
	if gr, ok := c.Strategy.(*GetRequest); ok {
		CPrintf("preprocess get to %d %+v", c.Rt.leader, gr.Args)
	} else if par, ok := c.Strategy.(*PutAppendRequest); ok {
		CPrintf("preprocess %s to %d %+v", par.Op, c.Rt.leader, par.Args)
	}
	for {
		ok, err := c.Strategy.Excecute()
		if ok && err == "" {
			break
		}
		if err == NOTALEADER || !ok {
			c.Rt.rollALeader()
		}
		if gr, ok := c.Strategy.(*GetRequest); ok {
			gr.Reply = &GetReply{}
		} else if par, ok := c.Strategy.(*PutAppendRequest); ok {
			par.Reply = &PutAppendReply{}
		}
		continue
	}
	var ret string
	if gr, ok := c.Strategy.(*GetRequest); ok {
		CPrintf("postprocess get to %d %+v", c.Rt.leader, gr.Reply)
		ret = gr.Reply.Value
	} else if par, ok := c.Strategy.(*PutAppendRequest); ok {
		CPrintf("postprocess %s to %d %+v", par.Op, c.Rt.leader, par.Reply)
	}
	return ret
}
