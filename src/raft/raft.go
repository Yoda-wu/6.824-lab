package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// HeartBeatTimeout 心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

// ApplyMsg
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           State         // peer's state
	electionTimeout time.Duration // 超时时间
	timer           *time.Ticker  // 计时器
	// Persistent attribute on all servers
	currentTerm int        // current term #nv
	voteFor     int        // candidateId #nv
	logs        []LogEntry // logs entries #nv

	// volatile attribute on all servers
	commitIndex int // index of the highest logs entry known to be committed
	lastApplied int // index of the highest logs entry applied to state machine

	// volatile arrtibute on leaders lab 2B
	nextIndex  []int // for each server,index of the next logs entry to send to that server
	matchIndex []int // for each server, index of the highest logs entry known to be replicated on server
	// apply chan lab 2B
	applyChan chan ApplyMsg
}

type State int // 节点的角色

const (
	Follower State = iota
	Candidate
	Leader
)

type VoteState int

const (
	Kill VoteState = iota
	Normal
	Expire
	Voted
)

type AppendState int

const (
	APPEND_KILL AppendState = iota
	APPEND_EXPIRE
	APPEND_SUCCESS
	APPEND_INCONSIS
	APPEND_COMMITED
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// 获取当前的任期
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	fmt.Printf("[	    func-GetState-rf(%+v)		] : the peer[%v], state is %v\n", rf.me, rf.me, rf.state)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int // term of candidate's last logs entry use string for temporary

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteState   VoteState
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Crash了
	if rf.killed() {
		reply.VoteState = Kill
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	// 候选人过期了
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 自己过期了
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1

	}

	// 没投票
	if rf.voteFor == -1 {
		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0
		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}
		if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
			reply.VoteState = Expire
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		rf.voteFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		fmt.Printf("[func-RequestVote-rf(%+v)] : vote to : %v\n", rf.me, rf.voteFor)

		rf.timer.Reset(rf.electionTimeout)

	} else {
		reply.VoteState = Voted

		reply.VoteGranted = false
		if rf.voteFor != args.CandidateId {
			return
		} else {
			rf.state = Follower
		}
		rf.timer.Reset(rf.electionTimeout)
	}

	return

}

type AppendEntriesArgs struct {
	Term         int        // leader's Term
	LeaderId     int        // follower can redirect clients
	PrevLogIndex int        // index of logs entry immediately preceding new ones
	PrevLogTerm  int        // term of  preLogIndex entry
	Entries      []LogEntry // logs entries to store (empty for heart beat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term        int // current term for leader to update itself
	AppendState AppendState
	NextIndex   int
	Success     bool // true if follower contained entry matching prevLogIndex and prevLogterm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// pass
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// crash 发生
	if rf.killed() {
		reply.AppendState = APPEND_KILL
		reply.Success = false
		reply.Term = -1
		return
	}
	// leader‘s term < follower's term - reject
	if rf.currentTerm > args.Term {
		reply.AppendState = APPEND_EXPIRE
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 落后于leader
	if args.PrevLogIndex > 0 && (len(rf.logs) < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.AppendState = APPEND_INCONSIS
		reply.Success = false
		reply.Term = rf.currentTerm
		// 因为apply的index 一定与leader保持一致，所以可以直接定位到这里，省去一步一步确定last agree index的时间
		reply.NextIndex = rf.lastApplied
		return
	}
	// 领先leader

	// 复制成功的场景
	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.timer.Reset(rf.electionTimeout)
	rf.state = Follower
	if rf.logs != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}
	reply.AppendState = APPEND_SUCCESS
	reply.Term = rf.currentTerm
	reply.Success = true

	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
	}
	return

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	if rf.state == Follower {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// 失败重传
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	fmt.Printf("[func-sendRequestVote-rf(%+v)->rf(%+v)] : vote rpc result: %v\n", rf.me, server, ok)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.VoteState {
	case Kill:
		return false
	case Expire:
		{
			rf.state = Follower
			rf.timer.Reset(rf.electionTimeout)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
			}
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNum <= len(rf.peers)/2 {
			*voteNum += 1
		}

		// 得票数超过一半
		if *voteNum >= len(rf.peers)/2 {
			*voteNum = 0
			if rf.state == Leader {
				return ok
			}
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			rf.timer.Reset(HeartBeatTimeout)
		}

	}

	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, successNum *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	fmt.Printf("[func-sendAppendEntries-rf(%+v)->rf(%+v)] :rpc result: %v\n", rf.me, server, ok)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.AppendState {
	case APPEND_KILL:
		{
			return false
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return index, term, false
	}
	// 不是leader直接返回
	if rf.state != Leader {
		return index, term, false
	}

	term = rf.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   len(rf.logs),
		Command: command,
	}

	rf.logs = append(rf.logs, entry)
	index = len(rf.logs)
	fmt.Printf("[func-Start-rf(%v+)]: index=%v, term=%v, isLeader=%v\n", rf.me, index, term, isLeader)
	return index, term, isLeader
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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		// 如果follower一段时间（`election timeout`）没收到心跳，那么他会认为系统中没有leader，然后开启选举
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.state {
			case Follower:

				rf.state = Candidate //follower先转换到candidate状态
				fallthrough          // 会直接执行紧跟着后面的一个case
			case Candidate:
				rf.currentTerm += 1 // candidate增加自己的当前任期号，
				rf.voteFor = rf.me  // 投票给自己
				voteNum := 1        // 统计票数
				// 选举时间会在(150-300 ms)之内随机选取。
				rf.electionTimeout = time.Duration(150+rand.Intn(200)) * time.Millisecond // 150 - 350 的随机超时时间
				rf.timer.Reset(rf.electionTimeout)
				// 并行地向集群中其他的服务器节点发送投票请求（RequestVote RPC）
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					voteArgs := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogIndex = rf.logs[len(rf.logs)-1].Term
					}
					fmt.Printf("[func-ticker-rf(%+v)] : ask rf[%v] for vote: \n", rf.me, i)

					voteReply := &RequestVoteReply{}
					go rf.sendRequestVote(i, voteArgs, voteReply, &voteNum)
				}
			case Leader:
				// 进行心跳
				successNum := 0 // 统计AppendEntries RPC正确返回的的节点数字
				rf.timer.Reset(HeartBeatTimeout)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArg := &AppendEntriesArgs{
						Entries:      nil,
						Term:         rf.currentTerm,
						PrevLogTerm:  0,
						PrevLogIndex: 0,
						LeaderCommit: rf.commitIndex,
						LeaderId:     rf.me,
					}
					appendEntriesReply := &AppendEntriesReply{}
					appendEntriesArg.Entries = rf.logs[rf.nextIndex[i]-1:]
					if rf.nextIndex[i] > 0 {
						appendEntriesArg.PrevLogIndex = rf.nextIndex[i] - 1
					}
					if appendEntriesArg.PrevLogIndex > 0 {
						appendEntriesArg.Term = rf.logs[appendEntriesArg.PrevLogIndex].Term
					}
					go rf.sendAppendEntries(rf.me, appendEntriesArg, appendEntriesReply, &successNum)
				}
			}
			rf.mu.Unlock()
		}

	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))  // leader maintains a nextIndex for each follower
	rf.matchIndex = make([]int, len(peers)) // leader maintains a matchIndex for each follower
	rf.applyChan = applyCh

	rf.state = Follower
	rf.electionTimeout = time.Duration(150+rand.Intn(200)) * time.Millisecond // 150 - 350 的随机超时时间
	rf.timer = time.NewTicker(rf.electionTimeout)
	// Your initialization code here (2A, 2B, 2C).
	fmt.Printf("[func-MakeRaftPeer-rf(%+v)] : is initing...\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
