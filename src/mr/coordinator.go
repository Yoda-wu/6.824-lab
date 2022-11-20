package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
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
	StartTime time.Time
	TaskAddr  *Task
}
type TaskMetaList struct {
	MetaList map[int]*TaskMetaInfo
}

// Your code here -- RPC handlers for the worker to call.

// check the task state is waiting or not
// if is waiting state, which means it is an initial task
func (list *TaskMetaList) checkTaskState(taskId int) bool {
	task, ok := list.MetaList[taskId]
	if !ok || task.TaskState != WaitingTaskState {
		return false
	}
	task.TaskState = WorkingTaskState
	task.StartTime = time.Now()
	return true
}

func (list *TaskMetaList) CheckTaskDone() bool {
	//fmt.Println(len(list.MetaList))
	var (
		mapTaskDoneNum      = 0
		reduceTaskDoneNum   = 0
		mapTaskUnDoneNum    = 0
		reduceTaskUnDoneNum = 0
	)
	for _, meta := range list.MetaList {
		//fmt.Printf("[checkTaskDone] meta=%v \n", meta)
		if meta.TaskAddr.TaskType == MapTask {
			if meta.TaskState != TaskDoneState {
				mapTaskUnDoneNum++
			} else {
				mapTaskDoneNum++
			}
		}
		if meta.TaskAddr.TaskType == ReduceTask {
			if meta.TaskState != TaskDoneState {
				reduceTaskUnDoneNum++
			} else {
				reduceTaskDoneNum++
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

func (list *TaskMetaList) addMetaTaskInfo(meta *TaskMetaInfo) bool {
	taskId := meta.TaskAddr.TaskId
	listMeta, _ := list.MetaList[taskId]
	if listMeta != nil {
		fmt.Println("task meta info is already in list")
		return false
	}
	list.MetaList[taskId] = meta
	return true

}

// Example an example RPC handler.
//
// Example the RPC argument and reply types are defined in rpc.go.
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
	_ = os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.JobState == JobAllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		ret = true
	}

	return ret
}

func (c *Coordinator) makeMapTask() {
	for _, file := range c.Files {

		task := Task{
			TaskId:    c.generateTaskId(),
			TaskType:  MapTask,
			ReduceNum: c.ReduceNum,
			FileName:  []string{file},
		}
		taskMetaInfo := &TaskMetaInfo{
			TaskState: WaitingTaskState,
			TaskAddr:  &task,
		}
		c.TaskMetaList.addMetaTaskInfo(taskMetaInfo)

		fmt.Printf("[makeMapTask] making a map task =%v \n", task)
		c.MapTaskChan <- &task
	}
}

func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.ReduceNum; i++ {
		fileNames := c.selectReduceNum(i)
		taskId := c.generateTaskId()
		task := Task{
			TaskId:    taskId,
			TaskType:  ReduceTask,
			ReduceNum: c.ReduceNum,
			FileName:  fileNames,
		}
		taskMetaInfo := &TaskMetaInfo{
			TaskState: WaitingTaskState,
			TaskAddr:  &task,
		}
		c.TaskMetaList.addMetaTaskInfo(taskMetaInfo)
		fmt.Printf("[makeMapTask] making a reduce task =%v \n", task)
		c.ReduceTaskChan <- &task
	}
}

func (c *Coordinator) selectReduceNum(reduceNum int) []string {
	var res []string
	path, _ := os.Getwd()        //Getwd返回一个对应当前工作目录的根路径。
	files, _ := os.ReadDir(path) // ReadDir 读取指定目录，返回按文件名排序的所有目录条目。
	for _, file := range files {
		flag := strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum))
		if flag {
			res = append(res, file.Name())
		}
	}
	return res
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
				*reply = *<-c.MapTaskChan
				fmt.Printf("[PollTask]poll the map task=%v \n", *reply)
				if !c.TaskMetaList.checkTaskState(reply.TaskId) {
					fmt.Println("current task is working")
				}
			} else {
				reply.TaskType = WaitingTask
				if c.TaskMetaList.CheckTaskDone() {
					// 如果所有Map任务都完成，就进入下一个阶段
					c.switchJobState()
					fmt.Println("All map task Done go to the next job state")
				}
				return nil
			}
		}
	case ReduceJobState:
		{
			if len(c.ReduceTaskChan) > 0 {
				*reply = *<-c.ReduceTaskChan
				fmt.Printf("[PollTask]poll the reduce task=%v \n", *reply)
				if !c.TaskMetaList.checkTaskState(reply.TaskId) {
					fmt.Println("current task is working")
				}
			} else {
				// 此时通道已经没有任务了
				reply.TaskType = WaitingTask
				if c.TaskMetaList.CheckTaskDone() {
					// 如果所有Reduce任务都完成，就进入下一个阶段
					c.switchJobState()
					fmt.Println("All reduce task Done go to the next job state")
				}
				return nil
			}
		}
	case JobAllDone:
		{
			reply.TaskType = DoneTask
		}
	default:
		return errors.New("error Job State")
	}
	return nil
}

func (c *Coordinator) switchJobState() {

	if c.JobState == MapJobState {
		// 初始化Reduce任务
		c.makeReduceTask()
		// 更改任务
		c.JobState = ReduceJobState
	} else if c.JobState == ReduceJobState {
		c.JobState = JobAllDone
	}
}

func (c *Coordinator) MarkTaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	taskType := args.TaskType
	taskId := args.TaskId
	switch taskType {
	case MapTask:
		{
			meta, ok := c.TaskMetaList.MetaList[taskId]
			if ok && meta.TaskState == WorkingTaskState {
				meta.TaskState = TaskDoneState
				fmt.Printf("[MarkTaskDone]map task(id=%v) is done\n", taskId)
			} else {
				fmt.Printf("[MarkTaskDone]map task(id=%v) was done already", taskId)
			}
		}
	case ReduceTask:
		{
			meta, ok := c.TaskMetaList.MetaList[taskId]
			if ok && meta.TaskState == WorkingTaskState {
				meta.TaskState = TaskDoneState
				fmt.Printf("[MarkTaskDone]reduce task(id=%v) is done\n", taskId)
			} else {
				fmt.Printf("[MarkTaskDone]reduce task(id=%v) was done already", taskId)
			}
		}
	default:
		fmt.Println("[MarkTaskDone] error task type")
	}
	return nil
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
		TaskMetaList: TaskMetaList{
			MetaList: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
		Files: files,
	}

	// Your code here.
	// 初始化map任务
	c.makeMapTask()

	c.server()
	go c.CrashDetect()
	return &c
}

func (c *Coordinator) CrashDetect() {
	// 启动一个循环来检测
	for {
		fmt.Println("Crash detecting......")
		time.Sleep(time.Second * 4)
		mu.Lock()
		// job 结束了则不需要进行检测。
		if c.JobState == JobAllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.TaskMetaList.MetaList {
			fmt.Printf("[CrashDetect] metalist len = %v checking task=%v\n", len(c.TaskMetaList.MetaList), v)
			if v.TaskState == WorkingTaskState && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAddr.TaskId, time.Since(v.StartTime))
				switch v.TaskAddr.TaskType {
				case MapTask:
					{
						c.MapTaskChan <- v.TaskAddr
						v.TaskState = WaitingTaskState
					}
				case ReduceTask:
					{
						c.ReduceTaskChan <- v.TaskAddr
						v.TaskState = WaitingTaskState
					}

				}
			}

		}
		mu.Unlock()
	}
}
