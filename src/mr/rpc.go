package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type FetchTaskArgs struct {
}

const MAPTASK = 0
const REDUCETASK = 1
const NOTASK = 2

type FetchTaskReply struct {
	TaskType  int
	FileName  string
	NMap      int //map task的数量
	NReduce   int // reduce task的数量
	MapNum    int // map task编号
	ReduceNum int //reduce task编号
}
type FinishTaskArgs struct {
	TaskType int
	TaskNum  int
}
type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
