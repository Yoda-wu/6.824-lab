package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义一个锁，用来处理worker访问master时的并法问题
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum      int          // 执行reduce任务的节点数
	JobState       JobState     // 整个Job的状态
	TaskStaticId   int          // 用于生成自增的任务id
	MapTaskChan    chan *Task   // Map任务的通道
	ReduceTaskChan chan *Task   // Reduce任务的通道
	TaskMetaList   TaskMetaList // 存储任务
	Files          []string     // 输入文件
}

type TaskMetaInfo struct {
	TaskState TaskState
	TaskAddr  *Task
}
type TaskMetaList struct {
	MetaList map[int]*TaskMetaInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c) // ？ Register怎么用
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
	ret := false
	// Your code here.

	if c.JobState == JobAllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		ret = true
	}

	return ret
}

func (c *Coordinator) makeMapTask() {
	for _, file := range c.Files {
		taskId := c.generateTaskId()
		task := Task{
			TaskId:   taskId,
			TaskType: MapTask,
			FileName: []string{file},
		}
		taskMetaInfo := &TaskMetaInfo{
			TaskState: WaitingTaskState,
			TaskAddr:  &task,
		}
		c.TaskMetaList.MetaList[taskId] = taskMetaInfo
		fmt.Printf("[makeMapTask] making a map task =%v \n", task)
		c.MapTaskChan <- &task
	}
}
func (c *Coordinator) generateTaskId() int {
	res := c.TaskStaticId
	c.TaskStaticId++
	return res
}

// PollTask 获取任务
func (c *Coordinator) PollTask(args *TaskRequest, reply *Task) error {

	mu.Lock()
	defer mu.Unlock()
	currentJobState := c.JobState
	switch currentJobState {
	case MapJobState:
		{
			if len(c.MapTaskChan) > 0 {
				reply = <-c.MapTaskChan
				fmt.Printf("[PollTask]poll the map task=%v \n", *reply)
				if !c.TaskMetaList.checkTaskState(reply.TaskId) {
					fmt.Println("current task is working")
				}
			} else {
				reply.TaskType = WaitingTask
				if c.TaskMetaList.CheckTaskDone() {
					fmt.Println("All task Done go to the next job state")
				}
				return nil
			}
		}
	default:
		return errors.New("error Job State")
	}
	return nil
}

func (list *TaskMetaList) checkTaskState(taskId int) bool {
	task, ok := list.MetaList[taskId]
	if !ok || task.TaskState != WaitingTaskState {
		return false
	}
	task.TaskState = WorkingTaskState
	return true
}

func (list *TaskMetaList) CheckTaskDone() bool {
	var (
		mapTaskDoneNum      = 0
		reduceTaskDoneNum   = 0
		mapTaskUnDoneNum    = 0
		reduceTaskUnDoneNum = 0
	)
	for _, meta := range list.MetaList {
		if meta.TaskAddr.TaskType == MapTask {
			if meta.TaskState != TaskDoneState {
				mapTaskUnDoneNum++
			} else {
				mapTaskDoneNum++
				break
			}
		}
		if meta.TaskAddr.TaskType == ReduceTask {
			if meta.TaskState != TaskDoneState {
				reduceTaskUnDoneNum++
			} else {
				reduceTaskDoneNum++
				break
			}
		}
	}
	if (mapTaskDoneNum > 0 && mapTaskUnDoneNum == 0) && (reduceTaskUnDoneNum == 0 && reduceTaskDoneNum == 0) {
		return true
	} else {
		if reduceTaskDoneNum > 0 && reduceTaskUnDoneNum == 0 {
			return true
		}
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum:      nReduce,
		TaskStaticId:   1,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		Files:          files,
	}

	// Your code here.
	// 初始化map任务
	c.makeMapTask()

	c.server()
	return &c
}
