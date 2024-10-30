package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2

const debug = true

// 单位：ms
// var electionTimeoutRange = [2]int{600, 900}

// const broadcastTimeout = 150
// const leaderCheckCommitTimeout = 75
var electionTimeoutRange = [2]int{250, 500}

const broadcastTimeout = 101
const leaderCheckCommitTimeout = 50

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	// all servers
	commitIndex       int
	lastApplied       int
	state             int
	electionTimer     *time.Timer // 选举超时计时器
	broadcastTimer    *time.Timer
	commitCheckerCond *sync.Cond
	snapshotLastIndex int
	snapshotLastTerm  int
	applier           *Applier
	// leaders
	nextIndex  []int
	matchIndex []int
}
type LogEntry struct {
	Term int
	Cmd  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister“.Save(“).
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	raftstate := w.Bytes()
	// rf.info(fmt.Sprintf("持久化log: %+v", rf.log))
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}
func (rf *Raft) persistWithSnapshot(data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	var snapshotLastIndex int
	var snapshotLastTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil {
		// rf.info("decode error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIndex = snapshotLastIndex
		rf.snapshotLastTerm = snapshotLastTerm
		// 更新commitIndex lastAppliedIndex
		if rf.commitIndex < rf.snapshotLastIndex {
			rf.commitIndex = rf.snapshotLastIndex
		}
		if rf.lastApplied < rf.snapshotLastIndex {
			rf.lastApplied = rf.snapshotLastIndex
		}
		// rf.LPrintf("复活，commitIndex=%d, snapshotLastIndex=%d, lastApplied=%d, snapshotLastTerm=%d", rf.commitIndex, rf.snapshotLastIndex, rf.lastApplied, rf.snapshotLastTerm)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotLastIndex || index > rf.logLastVIndex() {
		return
	}
	// rf.info(fmt.Sprintf("start taking a snaphot by index %d\n", index))

	// rf.log = rf.log[index-rf.snapshotLastIndex:]
	newLogSize := len(rf.log) - (index - rf.snapshotLastIndex)
	newLog := make([]LogEntry, newLogSize)
	copy(newLog, rf.log[index-rf.snapshotLastIndex:])
	rf.log = newLog

	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = rf.fetchLog(index).Term
	rf.persistWithSnapshot(snapshot)
	// rf.LPrintf("finish taking a snaphot by index %d\n", index)
}

// vIndex: 虚拟索引
func (rf *Raft) fetchLog(vIndex int) LogEntry {
	i := vIndex - rf.snapshotLastIndex
	if i > len(rf.log)-1 {
		i = len(rf.log) - 1
	} else if i < 0 {
		i = 0
	}
	return rf.log[i]
}
func (rf *Raft) logLastVIndex() int {
	return rf.snapshotLastIndex + len(rf.log) - 1
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
	Ok          bool //默认false，true表示成功进行了RPC
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Ok = true
	reply.Term = rf.currentTerm
	// 如果请求term>当前term，要更新votedFor为空，当前term也要更新
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.persist()
	}
	// 如果请求term<currentTerm，直接false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// 检查候选人的日志是否比当前节点更新
	lastLogIndex := rf.logLastVIndex()
	lastLogTerm := rf.fetchLog(lastLogIndex).Term
	logUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// 如果还没有投票给其他人，或者已经投票给了请求者，并且候选人的日志是最新的，则投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.resetElectionTimeout()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// rf.LPrintf("votedFor = %d", rf.votedFor)
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyCh chan RequestVoteReply) {

	reply := RequestVoteReply{}
	rf.peers[server].Call("Raft.RequestVote", args, &reply) //Call是同步的
	replyCh <- reply
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesResult struct {
	Term         int
	Success      bool
	XTerm        int
	XIndex       int
	XLen         int
	NeedSnapshot bool
}

// AppendEntries handler(receiver)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, result *AppendEntriesResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.LPrintf("from leader=%d,prevlogindex=%d,prevlogTerm=%d,lastLogIndex=%d,lastLogTerm=%d,leaderCommit=%d,mycommit=%d,entriesLen=%d", args.LeaderId, args.PrevLogIndex, args.PreLogTerm, rf.logLastVIndex(), rf.fetchLog(rf.logLastVIndex()).Term, args.LeaderCommit, rf.commitIndex, len(args.Entries))
	// rf.LPrintf("mylog:%+v", rf.log)
	result.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		result.Success = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	rf.resetElectionTimeout()
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// entries太新或冲突处理，leader端会降低prelogindex
	// follower's log is too short
	if args.PrevLogIndex > rf.logLastVIndex() {
		// 太新
		result.Success = false
		result.XLen = rf.logLastVIndex() + 1
		return
	} else if args.PrevLogIndex < rf.snapshotLastIndex {
		// for 3D
		result.NeedSnapshot = true
		result.Success = true
		return
	} else if rf.fetchLog(args.PrevLogIndex).Term != args.PreLogTerm {
		// term 冲突
		result.Success = false
		result.XTerm = rf.fetchLog(args.PrevLogIndex).Term
		xIndex := args.PrevLogIndex
		for ; xIndex > rf.commitIndex; xIndex-- {
			if rf.fetchLog(xIndex-1).Term != rf.fetchLog(xIndex).Term {
				break
			}
		}
		result.XIndex = xIndex
		return
	}

	if len(args.Entries) != 0 {
		i := 0
		for ; i < rf.logLastVIndex()-args.PrevLogIndex && i < len(args.Entries); i++ {
			if rf.fetchLog(args.PrevLogIndex+i+1).Term != args.Entries[i].Term {
				break
			}
		}
		if i < rf.logLastVIndex()-args.PrevLogIndex {
			newLog := make([]LogEntry, i+args.PrevLogIndex+1)
			copy(newLog, rf.log[:i+args.PrevLogIndex+1-rf.snapshotLastIndex])
			rf.log = newLog
		}
		if i < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
		rf.persist()
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		originalCommitIndex := rf.commitIndex
		// rf.commitIndex = min(args.LeaderCommit, len(args.Entries)+args.PrevLogIndex)
		rf.commitIndex = min(args.LeaderCommit, rf.logLastVIndex())
		// rf.LPrintf("更新commitIndex=%d", rf.commitIndex)
		if originalCommitIndex < rf.commitIndex {
			rf.commitCheckerCond.Signal()
		}
	}
	result.Success = true
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	result := AppendEntriesResult{}
	Ok := rf.peers[server].Call("Raft.AppendEntries", args, &result)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.LPrintf("recv from %d, result=%+v", server, result)
	if Ok && rf.state == LEADER && rf.currentTerm == args.Term {
		if result.Success {
			newMatchIndex := max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server]) // 可能在此期间安装了snapshot或者未来matchIndex已经更新
			rf.matchIndex[server] = newMatchIndex
			rf.nextIndex[server] = newMatchIndex + 1
			// rf.LPrintf("matchindex = %+v", rf.matchIndex)
		} else {
			if result.NeedSnapshot {
				go rf.sendSnapshotToPeer(server, args.Term, args.LeaderId, rf.snapshotLastIndex, rf.snapshotLastTerm)
			} else if result.Term > rf.currentTerm {
				// FOLLOWER的term比自己还大
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = result.Term
				rf.resetElectionTimeout()
				rf.persist()
			} else if result.XLen > 0 {
				// follower's log is too short
				rf.nextIndex[server] = result.XLen
			} else if result.XTerm == 0 {
				// follower的log完全冲突
				rf.nextIndex[server] = 1
			} else {
				leaderLastIndexForXTerm := 0
				for i := rf.logLastVIndex(); i >= rf.snapshotLastIndex; i-- {
					if rf.fetchLog(i).Term == result.XTerm {
						leaderLastIndexForXTerm = i
					}
				}
				if leaderLastIndexForXTerm != rf.snapshotLastIndex {
					rf.nextIndex[server] = leaderLastIndexForXTerm
				} else {
					rf.nextIndex[server] = result.XIndex
				}
			}
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastincludedTerm  int
	Data              []byte
}
type InstallSnapshotResult struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, result *InstallSnapshotResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	result.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimeout()
	// If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	hasEntry := false
	if rf.snapshotLastIndex+1 <= args.LastIncludedIndex &&
		args.LastIncludedIndex <= rf.logLastVIndex() {
		cutIndex := rf.logLastVIndex()
		for ; cutIndex > rf.snapshotLastIndex; cutIndex-- {
			if rf.fetchLog(cutIndex).Term == args.LastincludedTerm {
				hasEntry = true
				break
			}
		}
		if hasEntry {
			// rf.log = rf.log[cutIndex-rf.snapshotLastIndex:]
			newLog := make([]LogEntry, len(rf.log)-(cutIndex-rf.snapshotLastIndex))
			copy(newLog, rf.log[cutIndex-rf.snapshotLastIndex:])
			rf.log = newLog
		}
	}
	if !hasEntry {
		rf.log = []LogEntry{{Term: args.LastincludedTerm, Cmd: nil}}
	}
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastincludedTerm

	// if args.LastIncludedIndex > rf.lastApplied { // 提交snapshot后需要保证lastApplied = LastIncludedIndex
	// 	// rf.info(fmt.Sprintf("follower install snapshot by index %d\n", args.LastIncludedIndex))
	// 	rf.applyCh <- ApplyMsg{
	// 		CommandValid:  false,
	// 		SnapshotValid: true,
	// 		Snapshot:      args.Data,
	// 		SnapshotTerm:  args.LastincludedTerm,
	// 		SnapshotIndex: args.LastIncludedIndex,
	// 	}
	// 	rf.lastApplied = args.LastIncludedIndex
	// }
	// // 更新commitIndex
	// // rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	// rf.commitIndex = args.LastIncludedIndex
	// rf.LPrintf("commitIndex,LastIncludedIndex update to %d,%d", rf.commitIndex, rf.snapshotLastIndex)

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastincludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applier.Apply(msg)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persistWithSnapshot(args.Data)
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, result *InstallSnapshotResult) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, &result)
}

// the service using Raft (e.g. a k/v server) wants to starta
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer func() {
		// rf.resetBroadcastTimeout()
		resetTimer(rf.broadcastTimer, 0) // 立即发送appendEntriesRPC
		rf.mu.Unlock()
	}()
	if rf.state != LEADER || rf.killed() {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Cmd: command})
	rf.persist()
	// fmt.Printf("leader接受cmd: %+v, index=%d,me=%d\n", command, rf.logLastVIndex(), rf.me)
	// rf.info(fmt.Sprintf("leader接受cmd: %+v, index=%d", command, rf.logLastVIndex()))
	return rf.logLastVIndex(), rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applier.Kill()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			{
				rf.electionTickerHandler()
			}
		case <-rf.broadcastTimer.C:
			{
				rf.broadcastTickerHandler()
			}
		}
	}
}
func (rf *Raft) electionTickerHandler() {
	rf.mu.Lock()
	switch rf.state {
	case FOLLOWER:
		{
			go rf.campaign()
		}
	case CANDIDATE:
		{
			go rf.campaign()
		}
	case LEADER:
		{
			// do nothing
		}
	}
	rf.resetElectionTimeout()
	rf.mu.Unlock()
}
func (rf *Raft) broadcastTickerHandler() {
	rf.mu.Lock()
	if rf.state == LEADER {
		lastLogIndex := rf.logLastVIndex()
		term := rf.currentTerm
		leader := rf.me
		snapshotLastIndex := rf.snapshotLastIndex
		snapshotLastTerm := rf.snapshotLastTerm
		commitIndex := rf.commitIndex
		for peerId := range rf.peers {
			if peerId != rf.me {
				// 判断nextIndex
				peerNextIndex := rf.nextIndex[peerId]
				prevLogIndex := peerNextIndex - 1
				// if lastLogIndex >= rf.nextIndex[peerId] {
				// rf.info(fmt.Sprintf("prevLogIndex=%d for peer %d", prevLogIndex, peerId))
				if peerNextIndex <= rf.snapshotLastIndex {
					// need snapshot
					go rf.sendSnapshotToPeer(peerId, term, leader, snapshotLastIndex, snapshotLastTerm)
				} else {
					var entries []LogEntry
					PreLogTerm := rf.fetchLog(prevLogIndex).Term
					if lastLogIndex >= peerNextIndex { // 否则就是entries为空的心跳包
						entries = rf.log[peerNextIndex-rf.snapshotLastIndex:]
					}
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     leader,
						PrevLogIndex: prevLogIndex,
						PreLogTerm:   PreLogTerm,
						Entries:      entries,
						LeaderCommit: commitIndex,
					}
					// rf.info(fmt.Sprintf("当前log %+v", rf.log))
					// rf.info(fmt.Sprintf("向%d发送Entries nextIndex %d: %+v", peerId, peerNextIndex, rf.log[peerNextIndex-rf.snapshotLastIndex:]))
					// send
					go rf.sendAppendEntries(peerId, &args)
				}
			}
		}
	}
	rf.resetBroadcastTimeout()
	rf.mu.Unlock()
}
func (rf *Raft) sendSnapshotToPeer(peerId int, term int, leader int, snapshotLastIndex int, snapshotLastTerm int) {
	// rf.info(fmt.Sprintf("向%d发送snapshot\n", peerId))
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          leader,
		LastIncludedIndex: snapshotLastIndex,
		LastincludedTerm:  snapshotLastTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	result := InstallSnapshotResult{}
	ok := rf.sendInstallSnapshot(peerId, &args, &result)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if result.Term > rf.currentTerm {
			// rf.LPrintf("更改votedFor in send snapshot")
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.currentTerm = result.Term
			rf.persist()
		}
		rf.nextIndex[peerId] = rf.snapshotLastIndex + 1
	}
}
func (rf *Raft) leaderUpdateCommitIndexChecker() {
	for !rf.killed() {
		// 如果存在 N>commitIndex && 大部分 matchIndex[i]>=N && log[N].term==currentTerm
		// set commitIndex=N
		// 实际处理时N取[commitIndex+1,len(log)-1]
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		// rf.LPrintf("leader commitIndex=%d,logLastVIndex=%d", rf.commitIndex, rf.logLastVIndex())
		logLastVIndex := rf.logLastVIndex()
		commitIndex := rf.commitIndex
		if rf.commitIndex < logLastVIndex {
			N := logLastVIndex
			for ; N > commitIndex; N-- {
				consensus := 0
				for i := range rf.peers {
					// rf.LPrintf("match index for %d is %d", i, rf.matchIndex[i])
					if rf.matchIndex[i] >= N {
						consensus++
					}
				}
				if consensus >= rf.majority() && rf.fetchLog(N).Term == rf.currentTerm {
					rf.commitIndex = N
					// 通知commitChecker
					// rf.LPrintf("leader checked commit %d", N)
					rf.commitCheckerCond.Signal()
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(leaderCheckCommitTimeout))
	}
}
func (rf *Raft) commitChecker() {
	for !rf.killed() {
		/*
			两种情况触发signal：
			follower在appendEntriesRPC时更新commitIndex
			leader更新matchIndex
		*/
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.commitCheckerCond.Wait()
		}
		// commit的范围： [lastApplied+1,commitIndex]
		baseIndex := rf.snapshotLastIndex
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		// rf.LPrintf("commit util: %d loglast=%d,baseindex=%d", commitIndex, rf.logLastVIndex(), baseIndex)
		end := commitIndex + 1 - baseIndex
		if end > len(rf.log) {
			end = len(rf.log)
		}
		start := lastApplied + 1 - baseIndex
		if start < 0 {
			start = 0
		}
		entries := rf.log[start:end]
		// rf.mu.Unlock()
		for i, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandIndex: i + lastApplied + 1,
			}
			// rf.LPrintf("commit111 %+v", msg) // gotta data race but harmless
			// rf.applyCh <- msg
			// rf.LPrintf("commit222 %+v", msg) // gotta data race but harmless
			rf.applier.Apply(msg)
		}
		// rf.mu.Lock()
		// rf.lastApplied = max(commitIndex, rf.lastApplied)
		rf.lastApplied = commitIndex
		// rf.LPrintf("raft last applied = %d", rf.lastApplied)
		rf.mu.Unlock()
	}
}
func (rf *Raft) campaign() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.persist()
	// rf.info("发起选举")
	reqArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logLastVIndex(),
		LastLogTerm:  rf.fetchLog(rf.logLastVIndex()).Term,
	}
	replyCh := make(chan RequestVoteReply, len(rf.peers)) //reply管道，用于记录选票数量
	for peerIndex := range rf.peers {
		if peerIndex != rf.me {
			// 异步发送这些请求
			go rf.sendRequestVote(peerIndex, &reqArgs, replyCh)
		}
	}
	nVotes := 0 //收到的选票
	target := rf.majority()
	term := rf.currentTerm
	rf.mu.Unlock()
	for reply := range replyCh { // 从管道接受选票
		if reply.Ok {
			if reply.VoteGranted {
				nVotes += 1
				if nVotes >= target {
					rf.seizePower(term)
					break
				}
			} else if reply.Term > term {
				rf.mu.Lock()
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.persist()
				rf.mu.Unlock()
				break
			}
		}
	}
}
func (rf *Raft) seizePower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm { //历史回旋镖
		return
	}
	// rf.info("夺权")
	// rf.currentTerm += 1
	rf.state = LEADER
	rf.persist()
	// rf.LPrintf("snapshotLastIndex=%d,len(rf.log)=%d", rf.snapshotLastIndex, len(rf.log))
	initialNextIndex := rf.logLastVIndex() + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = initialNextIndex
		rf.matchIndex[i] = 0
	}
	go rf.leaderUpdateCommitIndexChecker()
}

func (rf *Raft) resetElectionTimeout() {
	resetTimer(rf.electionTimer, rf.getElectionTimeout())
}
func (rf *Raft) resetBroadcastTimeout() {
	resetTimer(rf.broadcastTimer, time.Duration(broadcastTimeout)*time.Millisecond)
}
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(electionTimeoutRange[1]-electionTimeoutRange[0])+electionTimeoutRange[0]) * time.Millisecond
}
func (rf *Raft) majority() int {
	// 自己的票不用计算
	return len(rf.peers) / 2
}
func (rf *Raft) info(info string) {
	if !debug {
		return
	}
	fmt.Printf("peer %d term %d state %d: %s\n", rf.me, rf.currentTerm, rf.state, info)
}
func (rf *Raft) LPrintf(fmtstr string, args ...any) {
	if !debug {
		return
	}
	prefix := fmt.Sprintf("peer %d term %d state %d: ", rf.me, rf.currentTerm, rf.state)
	fmt.Printf(prefix+fmtstr+"\n", args...)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// fmt.Printf("making raft me=%d,peers=%+v\n", me, peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// Raft中log index和term都是从1开始，peers下标从0开始
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 初始化一个junk log
	rf.log = []LogEntry{{Cmd: nil, Term: 1}}
	rf.state = FOLLOWER
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.broadcastTimer = time.NewTimer(time.Duration(broadcastTimeout) * time.Millisecond)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitCheckerCond = sync.NewCond(&rf.mu)
	rf.applier = MakeApplier(&applyCh)
	if rf.state == LEADER { //领导复活用
		go rf.leaderUpdateCommitIndexChecker()
	}
	// start ticker goroutine to start elections
	go rf.Ticker()
	go rf.commitChecker()
	return rf
}
