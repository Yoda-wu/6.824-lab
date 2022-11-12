package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskRequest worker请求coordinator的参数，无需其它参数
type TaskRequest struct{}

// Task 任务，也是worker请求coordinator的调度任务的响应
type Task struct {
	TaskType  TaskType // 任务类型
	TaskId    int      // 任务的id
	ReduceNum int      // Reduce任务的数量——用来哈希
	FileName  []string // 文件
}

// TaskType 任务的类型
type TaskType int

// JobState Job的状态，worker需要向coordinator获取当前整个Job的状态，来执行相应的过程。
type JobState int

// TaskState 任务目前的状态
type TaskState int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask

	DoneTask
)

// Job状态的枚举
const (
	MapJobState JobState = iota
	ReduceJobState
	JobAllDone
)

// Task状态的枚举
const (
	WorkingTaskState TaskState = iota
	WaitingTaskState
	TaskDoneState
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
