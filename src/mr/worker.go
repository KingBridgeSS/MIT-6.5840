package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := &FetchTaskReply{}
		ok := call("Coordinator.FetchTask", &FetchTaskArgs{}, &reply)
		if ok {
			switch reply.TaskType {
			case MAPTASK:
				{
					// 读取file并存为KeyValue
					inputFileName := reply.FileName
					file, err := os.Open(inputFileName)
					if err != nil {
						log.Fatalf("cannot open %v", inputFileName)
					}
					content, err := io.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", inputFileName)
					}
					file.Close()
					kva := mapf(inputFileName, string(content))
					// 生成中间文件mr-X-Y
					// 每一个相同的key对应一个确定的Y
					buckets := make([][]KeyValue, reply.NReduce)
					for i := 0; i < len(kva); i++ {
						index := ihash(kva[i].Key) % reply.NReduce
						buckets[index] = append(buckets[index], kva[i])
					}
					/*
						buckets
						[
							[{"key":"test","value":"1"},{"key":"test2","value":"1"}],
							...
						]
					*/
					for i, kva := range buckets {
						KVAJSON, _ := json.Marshal(kva)
						intermediateName := fmt.Sprintf("mr-%d-%d", reply.MapNum, i)
						writeStringToFileAtomic(intermediateName, string(KVAJSON))
					}
					call("Coordinator.FinishTask", &FinishTaskArgs{
						TaskType: MAPTASK,
						TaskNum:  reply.MapNum,
					}, &FinishTaskReply{})
				}
			case REDUCETASK:
				{
					/*
						同步处理所有mr - X - Y，其中Y = ReduceNum=hash(key)，遍历X
					*/
					mp := make(map[string][]string)
					for mapNum := 0; mapNum < reply.NMap; mapNum++ {
						intermediateName := fmt.Sprintf("mr-%d-%d", mapNum, reply.ReduceNum)
						var kva []KeyValue
						intermediateContent, _ := os.ReadFile(intermediateName)
						json.Unmarshal(intermediateContent, &kva)
						for _, kv := range kva {
							mp[kv.Key] = append(mp[kv.Key], kv.Value)
						}
					}
					var result strings.Builder
					for key, values := range mp {
						reduceResult := reducef(key, values)
						result.WriteString(fmt.Sprintf("%v %v\n", key, reduceResult))
					}
					resultFileName := fmt.Sprintf("mr-out-%d", reply.ReduceNum)
					writeStringToFileAtomic(resultFileName, result.String())
					call("Coordinator.FinishTask", &FinishTaskArgs{
						TaskType: REDUCETASK,
						TaskNum:  reply.ReduceNum,
					}, &FinishTaskReply{})
				}
			case NOTASK:
				{
					break
				}
			}
		} else {
			break
		}

	}
}
func writeStringToFileAtomic(filename string, content string) {
	f, _ := os.CreateTemp(".", filename+"*")
	f.WriteString(content)
	os.Rename(f.Name(), filename)
	f.Close()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	defer c.Close()
	if err != nil {
		os.Exit(0)
	}
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	os.Exit(0)
	return false
}
