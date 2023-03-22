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
	"6.824/labgob"
	"bytes"
	"log"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// HeartBeatTimeout 心跳超时时间
var HeartBeatTimeout = 25 * time.Millisecond

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
	timer           time.Time
	heartBeatTimer  *time.Ticker
	// Persistent attribute on all servers
	currentTerm int        // current term #nv
	voteFor     int        // candidateId #nv
	logs        []LogEntry // logs entries #nv

	// volatile attribute on all servers
	commitIndex      int // index of the highest logs entry known to be committed
	lastApplied      int // index of the highest logs entry applied to state machine
	lastIncludeIndex int
	lastIncludeTerm  int
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

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("[func-GetState-rf(%+v)] : the peer[%v], state is %v\n", rf.me, rf.me, rf.state)
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.voteFor)
	_ = e.Encode(rf.logs)
	_ = e.Encode(rf.lastIncludeIndex)
	_ = e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		DPrintf("fail to decode")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// ------------------------------------------- Snapshot部分 -------------------------------------------

type InstallSnapshotRequest struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte

	// 先不实现分块
	//done             bool
	//offset           int

}

type InstallSnapshotResponse struct {
	Term int
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
	//log.Printf("rf(%v) - call snapshot index=%v", rf.me, index)
	// Your code here (2D).
	if rf.killed() {
		log.Printf("rf(%v) is killed ", rf.me)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("rf(%v) - rf.lastIncludeIndex=%v before update", rf.me, rf.lastIncludeIndex)
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	newLogs := make([]LogEntry, 0)
	newLogs = append(newLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		newLogs = append(newLogs, rf.restoreLogEntry(i))
	}
	// 更新快照下标/任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}
	// 更新last include term
	//rf.lastIncludeTerm = rf.restoreLogTerm(index)
	rf.lastIncludeIndex = index
	rf.logs = newLogs

	rf.persister.SaveStateAndSnapshot(rf.persist(), snapshot)
	//log.Printf("rf(%v) - call lastIncludeIndex index=%v", rf.me, rf.lastIncludeIndex)

}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {
	// Your code here (2D)
	rf.mu.Lock()
	if rf.currentTerm > req.Term {
		resp.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = req.Term
	resp.Term = rf.currentTerm
	rf.state = Follower
	rf.voteFor = -1
	rf.persist()
	rf.timer = time.Now()

	if rf.lastIncludeIndex >= req.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}
	index := req.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLogEntry(i))
	}
	rf.lastIncludeTerm = req.LastIncludeTerm
	rf.lastIncludeIndex = req.LastIncludeIndex
	rf.logs = tempLog

	rf.persister.SaveStateAndSnapshot(rf.persist(), req.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
		Snapshot:      req.Data,
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	rf.mu.Unlock()
	rf.applyChan <- msg
}

func (rf *Raft) invokeInstallSnapshot(server int) {
	rf.mu.Lock()
	req := InstallSnapshotRequest{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	resp := InstallSnapshotResponse{}
	rf.mu.Unlock()
	res := rf.sendInstallSnapshot(server, &req, &resp)
	if res {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != req.Term {
			rf.mu.Unlock()
			return

		}
		if resp.Term > rf.currentTerm {
			rf.state = Follower
			rf.voteFor = -1
			rf.persist()
			rf.timer = time.Now()
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[server] = req.LastIncludeIndex
		rf.nextIndex[server] = req.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendInstallSnapshot(server int, req *InstallSnapshotRequest, resp *InstallSnapshotResponse) bool {
	// Your code here (2D)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", req, resp)
	return ok
}

//------------------------------------------- Snapshot部分end -------------------------------------------

// ------------------------------------------- RequestVote部分 -------------------------------------------

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int // term of candidate's last logs entry use string for temporary

}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		nowTime := time.Now()
		time.Sleep(time.Duration(rf.generateTimeOut(int64(rf.me))) * time.Millisecond)
		//afterSleep := time.Now()
		rf.mu.Lock()
		//DPrintf("[rf(%v)-ticker]-sleep time=%v time is before=%v\n", rf.me, afterSleep.Sub(nowTime), rf.timer.Before(nowTime))

		if rf.timer.Before(nowTime) && rf.state != Leader {
			//DPrintf("[rf(%v)-ticker]-Start election timer=%v\n", rf.me, rf.timer)
			rf.state = Candidate
			rf.currentTerm++
			rf.voteFor = rf.me
			//voteNum := 1 // 统计票数
			rf.persist()
			rf.invokeRequestVote()
			rf.timer = time.Now()
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) invokeRequestVote() {
	voteNum := 1 // 统计票数
	// 并行地向集群中其他的服务器节点发送投票请求（RequestVote RPC）
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, voteNum *int) {
			rf.mu.Lock()
			voteArgs := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastIndex(),
				LastLogTerm:  rf.getLastTerm(),
			}

			//log.Printf("[func-ticker-rf(%+v)] : ask rf[%v] for vote: args:=%v \n", rf.me, server, voteArgs)

			voteReply := &RequestVoteReply{}
			//log.Printf("[func-ticker-rf(%+v)] : before unlock ask rf[%v] for vote: args:=%v \n", rf.me, server, voteArgs)

			rf.mu.Unlock()
			//log.Printf("[func-ticker-rf(%+v)] :after unlock ask rf[%v] for vote: args:=%v \n", rf.me, server, voteArgs)

			res := rf.sendRequestVote(server, voteArgs, voteReply)
			//DPrintf("[func-ticker-rf(%+v)]send  vote to server(%v) res :=%v \n ", rf.me, server, res)
			if res {
				rf.mu.Lock()
				if rf.state != Candidate || voteArgs.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if voteReply.Term > voteArgs.Term {
					if voteReply.Term < rf.currentTerm {
						rf.currentTerm = voteReply.Term
					}
					rf.state = Follower
					rf.voteFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}
				// 成功投票
				if voteReply.VoteGranted && voteReply.Term == voteArgs.Term {
					*voteNum++
					if *voteNum > len(rf.peers)/2 {
						rf.state = Leader
						rf.voteFor = -1
						rf.persist()
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()
						rf.timer = time.Now()
						rf.mu.Unlock()
						return

					}
				}
				rf.mu.Unlock()
				return

			}
		}(i, &voteNum)

	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//var voteNum *int
	//log.Println(rf.me, "sendRequestVote")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//log.Println(rf.me, "sendRequestVote", ok)

	//return ok
	return ok
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("[func-RequestVote-rf(%+v)] : args=%v \n", rf.me, args)

	// Crash了
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	// 候选人过期了
	if args.Term < rf.currentTerm {
		//log.Printf("[func-RequestVote-rf(%+v)] : args.Term=%v, rf.currentTerm=%v candidate is expire \n", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 自己过期了
	if args.Term > rf.currentTerm {
		//log.Printf("[func-RequestVote-rf(%+v)] : args.Term=%v, rf.currentTerm=%v self is expire \n", rf.me, args.Term, rf.currentTerm)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}

	// 没投票并且uptodate
	if rf.voteFor == -1 && rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {

		rf.voteFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		rf.timer = time.Now()
		//log.Printf("[func-RequestVote-rf(%+v)] : vote to : %v\n", rf.me, rf.voteFor)
		return
	} else {
		//log.Printf("[func-RequestVote-rf(%+v)] : rf.voteFor=%v\n", rf.me, rf.voteFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

}

// ------------------------------------------- AppendEntries部分 -------------------------------------------

type AppendEntriesArgs struct {
	Term         int        // leader's Term
	LeaderId     int        // follower can redirect clients
	PrevLogIndex int        // index of logs entry immediately preceding new ones
	PrevLogTerm  int        // term of  preLogIndex entry
	Entries      []LogEntry // logs entries to store (empty for heart beat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term      int // current term for leader to update itself
	NextIndex int
	Success   bool // true if follower contained entry matching prevLogIndex and prevLogterm
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		time.Sleep(HeartBeatTimeout)
		rf.mu.Lock()
		if rf.state == Leader {
			//DPrintf("leader rf(%v) send heartBeat", rf.me)
			rf.invokeAppendEntries()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) invokeAppendEntries() {
	successNum := 1 // 统计AppendEntries RPC正确返回的的节点数字
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, successNum *int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			args := &AppendEntriesArgs{
				Entries:      nil,
				Term:         rf.currentTerm,
				PrevLogTerm:  0,
				PrevLogIndex: 0,
				LeaderCommit: rf.commitIndex,
				LeaderId:     rf.me,
			}
			reply := &AppendEntriesReply{}
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				//log.Printf("server(%v) need to  InstallSnapshot", server)
				go rf.invokeInstallSnapshot(server)
				rf.mu.Unlock()
				return
			}
			prevLogIndex, preLogTerm := rf.getPrevLogInfo(server)
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = preLogTerm
			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			rf.mu.Unlock()

			res := rf.sendAppendEntries(server, args, reply, successNum)
			//log.Printf("leader(%v) send heart Beat to rf(%v) res=%v", rf.me, server, res)
			if res {
				//log.Printf("leader(%v) send heart Beat to rf(%v) res=%v before process", rf.me, server, res)

				rf.mu.Lock()

				// leader 落后
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.state = Follower
					rf.persist()
					rf.timer = time.Now()
					//DPrintf("[func-sendAE-rf(%v)]: append fail:  leader is behind\n", rf.me)
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					//DPrintf("[func-sendAE-rf(%v)]: append rf(%v) success\n", rf.me, server)

					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					commitIndex := rf.lastApplied
					for idx := rf.getLastIndex(); idx >= rf.lastIncludeIndex+1; idx-- {
						count := 0

						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								count++
								continue
							}
							if rf.matchIndex[i] >= idx {
								count++
							}
						}
						//log.Printf("FUNC-AE-LD(%v) count = %v , len(rf.peers)=%v ,idx=%v", rf.me, count, len(rf.peers), idx)

						// leader只提交当前任期的日志
						if count > len(rf.peers)/2 && rf.restoreLogTerm(idx) == rf.currentTerm {
							//log.Printf("FUNC-AE-LD(%v) count = %v , len(rf.peers)=%v ,idx=%v", rf.me, count, len(rf.peers), idx)
							commitIndex = idx
							//log.Printf("FUNC-AE-LD(%v) count = %v , len(rf.peers)=%v ,commitIndex=%v", rf.me, count, len(rf.peers), commitIndex)

							if commitIndex == rf.lastApplied {
								//log.Printf("FUNC-AE-LD(%v) count = %v , len(rf.peers)=%v ,commitIndex=%v", rf.me, count, len(rf.peers), commitIndex)
								rf.mu.Unlock()
								return
							}
							rf.commitIndex = commitIndex
							//log.Printf("FUNC-AE-LD(%v) count = %v , len(rf.peers)=%v ,rf.commitIndex=%v", rf.me, count, len(rf.peers), rf.commitIndex)
							applyMsgs := make([]ApplyMsg, 0)
							// 提交
							for rf.lastApplied < rf.commitIndex {
								//DPrintf("[func-sendAE-rf(%v)]: before apply success:  rf.lastApplied=%v\n", rf.me, rf.lastApplied)

								rf.lastApplied++
								msg := ApplyMsg{
									CommandValid: true,
									CommandIndex: rf.lastApplied,
									Command:      rf.restoreLogEntry(rf.lastApplied).Command,
								}
								//DPrintf("[func-sendAE-rf(%v)]: apply msg=%v\n", rf.me, msg)

								applyMsgs = append(applyMsgs, msg)
								//DPrintf("[func-sendAE-rf(%v)]: after apply success:  rf.lastApplied=%v\n", rf.me, rf.lastApplied)

							}
							rf.mu.Unlock()
							for _, msg := range applyMsgs {
								rf.applyChan <- msg
							}
							//DPrintf("[func-sendAE-rf(%v)]: append success:  leaderCommit=%v\n", rf.me, rf.commitIndex)
							//DPrintf("[func-sendAE-rf(%v)]: append success: rf.logs=%v leaderCommit=%v\n", rf.me, rf.logs, rf.commitIndex)
							return

						}
					}

				} else { // 出现冲突
					if reply.NextIndex != -1 {
						rf.nextIndex[server] = reply.NextIndex
					}
				}
				rf.mu.Unlock()
				return

			}
		}(i, &successNum)

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, successNum *int) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("[func-sendAppendEntries-rf(%+v)->rf(%+v)] :rpc result: %v\n", rf.me, server, ok)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("[func-AppendEntries-rf(%v)]:args =%v rf.logs=%v\n", rf.me, args, rf.logs)
	// pass
	rf.mu.Lock()
	// crash 发生
	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		rf.mu.Unlock()
		return
	}
	// leader‘s term < follower's term - reject
	if rf.currentTerm > args.Term {
		//log.Printf("leader(%v).term = %v, is smaller than rf(%v).term=%v", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = -1
		rf.mu.Unlock()
		return
	}

	reply.Success = true
	reply.Term = args.Term
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.persist()
	rf.timer = time.Now()

	if rf.lastIncludeIndex > args.PrevLogIndex {
		//log.Printf("(rf%v)args.PrevLogIndex =%v rf.lastIncludeIndex = %v\n", rf.me, args.PrevLogIndex, rf.lastIncludeIndex)
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		rf.timer = time.Now()
		rf.mu.Unlock()
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		//log.Printf("(rf%v)args.PrevLogIndex =%v rf.getLastIndex = %v\n", rf.me, args.PrevLogIndex, rf.getLastIndex())
		reply.Success = false
		reply.NextIndex = rf.getLastIndex()
		rf.mu.Unlock()
		return
	}
	if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
		for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
			if rf.restoreLogTerm(index) != tempTerm {
				reply.NextIndex = index
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
		return
	}

	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()
	//fmt.Printf("[func-AppendEntries-rf(%v)]: after replicated rf(%v).logs =%v\n", rf.me, rf.me, rf.logs)
	rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	applyMsgs := make([]ApplyMsg, 0)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.restoreLogEntry(rf.lastApplied).Command,
		}
		applyMsgs = append(applyMsgs, applyMsg)
		//DPrintf("rf(%v) apply log=%v\n", rf.me, applyMsg)
	}
	rf.mu.Unlock()
	for _, msg := range applyMsgs {
		rf.applyChan <- msg
	}
	//DPrintf("[func-AppendEntries-rf(%v)]: append success: reply=%v rf.logs=%v leaderCommit=%v\n", rf.me, *reply, rf.logs, args.LeaderCommit)
	//fmt.Println()
	//DPrintf("[func-AppendEntries-rf(%v)]: append success: args.Entries=%v leader is =%v, leaderCommit=%v\n", rf.me, args.Entries, args.LeaderId, args.LeaderCommit)
	//fmt.Println()
	//fmt.Printf("[func-AppendEntries-rf(%v)]: append success: reply=%v\n", rf.me, *reply)
	return

}

// ------------------------------------------- AppendEntries部分结束 -------------------------------------------

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{}) // 往所有节点的日志前加一个节点，让下标从1开始。
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))  // leader maintains a nextIndex for each follower
	rf.matchIndex = make([]int, len(peers)) // leader maintains a matchIndex for each follower
	rf.applyChan = applyCh

	rf.state = Follower
	rf.electionTimeout = time.Duration(rf.generateTimeOut(int64(rf.me))) * time.Millisecond // 150 - 350 的随机超时时间
	rf.heartBeatTimer = time.NewTicker(HeartBeatTimeout)

	rf.lastIncludeTerm = 0
	rf.lastIncludeIndex = 0

	rf.mu.Unlock()
	// Your initialization code here (2A, 2B, 2C).
	//fmt.Printf("[func-MakeRaftPeer-rf(%+v)] : is initing...\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludeIndex != 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.heartBeat()
	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	// 不是leader直接返回
	if rf.state != Leader {
		return -1, -1, false
	}
	//DPrintf("[func-Start-rf(%v)]: command=%v\n ", rf.me, command)
	index = rf.getLastIndex() + 1
	term = rf.currentTerm
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	rf.logs = append(rf.logs, entry)
	rf.persist()
	//DPrintf("[func-Start-rf(%v+)]: index=%v, term=%v, logEntry=%v\n ", rf.me, index, term, rf.logs)
	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
