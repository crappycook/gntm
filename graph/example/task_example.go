package example

import (
	"context"
	"fmt"
	"time"

	"workflow/graph"
)

func RunTaskExample() {
	// 创建任务图
	taskGraph := graph.NewTaskGraph()

	// 任务1：获取用户信息
	getUserTask := &graph.Task{
		ID: "get_user",
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			// 模拟RPC调用获取用户信息
			time.Sleep(100 * time.Millisecond)
			return map[string]interface{}{
				"user_id": 123,
				"name":    "John Doe",
			}, nil
		},
		Status: graph.TaskStatusPending,
	}

	// 任务2：获取用户订单
	getOrdersTask := &graph.Task{
		ID:      "get_orders",
		Depends: []*graph.Task{getUserTask},
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			// 使用用户信息获取订单
			userData := inputs["get_user"].(map[string]interface{})
			userID := userData["user_id"]

			// 模拟RPC调用获取订单
			time.Sleep(200 * time.Millisecond)
			return []map[string]interface{}{
				{"order_id": 1, "user_id": userID, "amount": 100},
				{"order_id": 2, "user_id": userID, "amount": 200},
			}, nil
		},
		Status: graph.TaskStatusPending,
	}

	// 任务3：获取用户积分
	getPointsTask := &graph.Task{
		ID:      "get_points",
		Depends: []*graph.Task{getUserTask},
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			// 使用用户信息获取积分
			userData := inputs["get_user"].(map[string]interface{})
			userID := userData["user_id"]

			// 模拟RPC调用获取积分
			time.Sleep(150 * time.Millisecond)
			return map[string]interface{}{
				"user_id": userID,
				"points":  1000,
			}, nil
		},
		Status: graph.TaskStatusPending,
	}

	// 任务4：汇总用户信息
	summarizeTask := &graph.Task{
		ID:      "summarize",
		Depends: []*graph.Task{getOrdersTask, getPointsTask},
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			orders := inputs["get_orders"].([]map[string]interface{})
			points := inputs["get_points"].(map[string]interface{})

			return map[string]interface{}{
				"total_orders": len(orders),
				"points":       points["points"],
			}, nil
		},
		Status: graph.TaskStatusPending,
	}

	// 添加所有任务到图中
	if err := taskGraph.AddTask(getUserTask); err != nil {
		fmt.Printf("Failed to add getUserTask: %v\n", err)
		return
	}
	if err := taskGraph.AddTask(getOrdersTask); err != nil {
		fmt.Printf("Failed to add getOrdersTask: %v\n", err)
		return
	}
	if err := taskGraph.AddTask(getPointsTask); err != nil {
		fmt.Printf("Failed to add getPointsTask: %v\n", err)
		return
	}
	if err := taskGraph.AddTask(summarizeTask); err != nil {
		fmt.Printf("Failed to add summarizeTask: %v\n", err)
		return
	}

	// 获取执行顺序
	order, err := taskGraph.GetExecutionOrder()
	if err != nil {
		fmt.Printf("Error getting execution order: %v\n", err)
		return
	}
	fmt.Printf("Execution Order: %v\n", order)

	// 执行任务图
	ctx := context.Background()

	data, err := taskGraph.Execute(ctx, graph.WithWorkerCount(3), graph.WithDebugLog(true))
	if err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		return
	}

	fmt.Printf("Final result: %+v\n", data)
}
