package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true
const (
	MinTimeout = 75
	MaxTimeout = 100
)

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// getLastIndex 获取的是最后的log index  而不是下标
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludeIndex
}

// getLastTerm 获取的是日志数组的最后一个日志的任期
func (rf *Raft) getLastTerm() int {
	if len(rf.logs)-1 == 0 {
		return rf.lastIncludeTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

// UpToDate 判断日志index和term是不是at least up to date
func (rf *Raft) UpToDate(index, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

// generateTimeOut 随机生成选举超时时间
func (rf *Raft) generateTimeOut(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return MinTimeout + rand.Intn(MaxTimeout)
}

// restoreLogEntry 根据快照偏移获取对应log index的日志
func (rf *Raft) restoreLogEntry(curIndex int) LogEntry {
	// 这里需要减去快照存储的便宜
	return rf.logs[curIndex-rf.lastIncludeIndex]
}

// restoreLogTerm 根据快照偏移获取对应log index 的任期
func (rf *Raft) restoreLogTerm(curIndex int) int {
	if curIndex-rf.lastIncludeIndex == 0 {
		return rf.lastIncludeTerm
	}
	return rf.logs[curIndex-rf.lastIncludeIndex].Term
}

// getPrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if lastIndex+1 == prevLogIndex {
		prevLogIndex = lastIndex
	}
	return prevLogIndex, rf.restoreLogTerm(prevLogIndex)
}

func LogContain(entries []LogEntry, entry LogEntry) bool {
	for _, e := range entries {
		if e.Index >= entry.Index && e.Term >= entry.Term {
			return true
		}

	}
	return false
}
