package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/dominikbraun/graph"
	"golang.org/x/sync/errgroup"
)

// TaskStatus 表示任务的状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusSkipped   TaskStatus = "skipped"
)

// TaskResult 表示任务执行的结果
type TaskResult struct {
	Data  interface{}
	Error error
}

// Task 表示一个可执行的任务
type Task struct {
	ID        string
	Execute   func(ctx context.Context, inputs map[string]interface{}) (interface{}, error)
	Depends   []*Task
	Status    TaskStatus
	Condition func(inputs map[string]interface{}) bool
}

// TaskGraph 表示任务的DAG图
type TaskGraph struct {
	graph      graph.Graph[string, *Task]
	taskLayers map[string]int // 存储任务的层级
}

// NewTaskGraph 创建新的任务图
func NewTaskGraph() *TaskGraph {
	return &TaskGraph{
		graph:      graph.New(func(task *Task) string { return task.ID }, graph.Directed()),
		taskLayers: make(map[string]int),
	}
}

// AddTask 添加新任务到图中
func (tg *TaskGraph) AddTask(task *Task) error {
	// 添加节点
	if err := tg.graph.AddVertex(task); err != nil {
		return fmt.Errorf("failed to add task: %v", err)
	}

	// 添加边（依赖关系）并计算层级
	if len(task.Depends) == 0 {
		// 没有依赖的任务在第0层
		tg.taskLayers[task.ID] = 0
	} else {
		// 添加边并找出最大依赖层级
		maxDepLayer := -1
		for _, dep := range task.Depends {
			if err := tg.graph.AddEdge(dep.ID, task.ID); err != nil {
				return fmt.Errorf("failed to add dependency: %v", err)
			}
			// 获取依赖的层级
			if layer, exists := tg.taskLayers[dep.ID]; exists {
				if layer > maxDepLayer {
					maxDepLayer = layer
				}
			} else {
				return fmt.Errorf("dependency task %s not found in layer map", dep.ID)
			}
		}
		// 当前任务的层级是其依赖的最大层级 + 1
		tg.taskLayers[task.ID] = maxDepLayer + 1
	}

	// 检查是否有环
	if _, err := graph.TopologicalSort(tg.graph); err != nil {
		return fmt.Errorf("invalid task graph: %v", err)
	}

	return nil
}

// ExecuteOptions 定义执行选项
type ExecuteOptions struct {
	WorkerCount int
}

// executeLayer 执行单层任务并返回结果
func (tg *TaskGraph) executeLayer(ctx context.Context, layer []string, results map[string]interface{}) (map[string]interface{}, error) {
	g, ctx := errgroup.WithContext(ctx)
	layerResults := make(map[string]interface{})
	var layerMu sync.Mutex

	// 并行执行同一层的任务
	for _, taskID := range layer {
		taskID := taskID
		task, _ := tg.graph.Vertex(taskID)

		g.Go(func() error {
			// 收集任务的输入（来自依赖任务的结果）
			inputs := make(map[string]interface{})
			for _, dep := range task.Depends {
				if result, ok := results[dep.ID]; ok {
					inputs[dep.ID] = result
				}
			}

			// 检查条件是否满足
			if task.Condition != nil && !task.Condition(inputs) {
				task.Status = TaskStatusSkipped
				return nil
			}

			// 更新任务状态并执行
			task.Status = TaskStatusRunning
			result, err := task.Execute(ctx, inputs)
			if err != nil {
				task.Status = TaskStatusFailed
				return fmt.Errorf("task %s failed: %v", taskID, err)
			}

			task.Status = TaskStatusCompleted
			// 加锁保护并发写入
			layerMu.Lock()
			layerResults[taskID] = result
			layerMu.Unlock()
			return nil
		})
	}

	// 等待当前层的所有任务完成
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return layerResults, nil
}

// Execute 执行整个任务图
func (tg *TaskGraph) Execute(ctx context.Context, opts ExecuteOptions) (map[string]interface{}, error) {
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = 5
	}

	// 获取执行顺序
	_, err := graph.TopologicalSort(tg.graph)
	if err != nil {
		return nil, fmt.Errorf("failed to sort tasks: %v", err)
	}

	// 创建结果映射表
	results := make(map[string]interface{})

	// 找出最大层级
	maxLayer := 0
	for _, layer := range tg.taskLayers {
		if layer > maxLayer {
			maxLayer = layer
		}
	}
	fmt.Println("task layers: ", tg.taskLayers)

	// 按层级组织任务
	layers := make([][]string, maxLayer+1)
	for taskID, layer := range tg.taskLayers {
		layers[layer] = append(layers[layer], taskID)
	}

	fmt.Println("layers: ", layers)

	// 按层次执行任务
	for _, layer := range layers {
		layerResults, err := tg.executeLayer(ctx, layer, results)
		if err != nil {
			return nil, err
		}

		// 合并当前层的结果
		for k, v := range layerResults {
			results[k] = v
		}
	}

	return results, nil
}

// GetExecutionOrder 获取任务的执行顺序
func (tg *TaskGraph) GetExecutionOrder() ([]string, error) {
	return graph.TopologicalSort(tg.graph)
}

// GetTaskStatus 获取任务状态
func (tg *TaskGraph) GetTaskStatus(taskID string) (TaskStatus, error) {
	task, err := tg.graph.Vertex(taskID)
	if err != nil {
		return "", fmt.Errorf("task %s not found", taskID)
	}

	return task.Status, nil
}
