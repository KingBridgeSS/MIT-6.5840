package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const NOT_STARTED = 0
const WORKING = 1
const DONE = 2

type Coordinator struct {
	// Your definitions here.
	lock               sync.Mutex
	files              []string
	mapTaskStatuses    []int
	reduceTaskStatuses []int
	nMap               int
	nReduce            int
	mapDoneNum         int // map任务完成数量
	reduceDoneNum      int
	mapFinished        bool
	reduceFinished     bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.mapFinished {
		mapNum := 0
		for mapNum < c.nMap && c.mapTaskStatuses[mapNum] != NOT_STARTED {
			mapNum += 1
		}
		if mapNum == c.nMap {
			reply.TaskType = NOTASK
		} else {
			c.mapTaskStatuses[mapNum] = WORKING
			reply.TaskType = MAPTASK
			reply.FileName = c.files[mapNum]
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.MapNum = mapNum
			// 10s不完成任务就错误恢复
			go func() {
				time.Sleep(time.Second * 10)
				c.lock.Lock()
				if c.mapTaskStatuses[mapNum] != DONE {
					c.mapTaskStatuses[mapNum] = NOT_STARTED
				}
				c.lock.Unlock()
			}()
		}
	} else if !c.reduceFinished {
		reduceNum := 0
		for reduceNum < c.nReduce && c.reduceTaskStatuses[reduceNum] != NOT_STARTED {
			reduceNum += 1
		}
		if reduceNum == c.nReduce {
			reply.TaskType = NOTASK
		} else {
			c.reduceTaskStatuses[reduceNum] = WORKING
			reply.TaskType = REDUCETASK
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.ReduceNum = reduceNum
			go func() {
				time.Sleep(time.Second * 10)
				c.lock.Lock()
				if c.reduceTaskStatuses[reduceNum] != DONE {
					c.reduceTaskStatuses[reduceNum] = NOT_STARTED
				}
				c.lock.Unlock()
			}()
		}
	} else {
		reply.TaskType = NOTASK
	}
	return nil
}
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch args.TaskType {
	case MAPTASK:
		{
			// 由于错误恢复机制，这里可能提交同样的任务。仅接受第一个提交的任务即可
			if c.mapTaskStatuses[args.TaskNum] != DONE {
				c.mapDoneNum += 1
				c.mapTaskStatuses[args.TaskNum] = DONE
				if c.mapDoneNum == c.nMap {
					c.mapFinished = true
				}
			}
		}
	case REDUCETASK:
		{
			if c.reduceTaskStatuses[args.TaskNum] != DONE {
				c.reduceDoneNum += 1
				c.reduceTaskStatuses[args.TaskNum] = DONE
				if c.reduceDoneNum == c.nReduce {
					c.reduceFinished = true
				}
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.reduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	// bool's default value is false
	c.mapTaskStatuses = make([]int, c.nMap)
	c.reduceTaskStatuses = make([]int, c.nReduce)
	c.mapFinished = false
	c.reduceFinished = false
	c.mapDoneNum = 0
	c.reduceDoneNum = 0

	c.server()
	return &c
}
