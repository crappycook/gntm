package example

import (
	"context"
	"fmt"

	"workflow/graph"
)

func RunConditionalExample() {
	taskGraph := graph.NewTaskGraph()

	// 任务1：获取用户信息
	getUserTask := &graph.Task{
		ID: "get_user",
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			return map[string]interface{}{
				"user_id":   123,
				"vip_level": 2, // VIP等级
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

			return []map[string]interface{}{
				{"order_id": 1, "user_id": userID, "amount": 100},
				{"order_id": 2, "user_id": userID, "amount": 200},
			}, nil
		},
		Status: graph.TaskStatusPending,
	}

	// 任务3：获取VIP特权信息（只有VIP等级大于1才执行）
	getVIPPrivilegesTask := &graph.Task{
		ID:      "get_vip_privileges",
		Depends: []*graph.Task{getUserTask},
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			return map[string]interface{}{
				"discount_rate":  0.8,
				"special_offers": []string{"birthday_gift", "free_shipping"},
			}, nil
		},
		// 条件执行：只有VIP等级>1才执行
		Condition: func(inputs map[string]interface{}) bool {
			userData := inputs["get_user"].(map[string]interface{})
			vipLevel := userData["vip_level"].(int)
			return vipLevel > 1
		},
		Status: graph.TaskStatusPending,
	}

	// 任务4：计算订单优惠（依赖于订单信息和VIP特权信息）
	calculateDiscountTask := &graph.Task{
		ID:      "calculate_discount",
		Depends: []*graph.Task{getOrdersTask, getVIPPrivilegesTask},
		Execute: func(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
			orders := inputs["get_orders"].([]map[string]interface{})
			vipPrivileges := inputs["get_vip_privileges"].(map[string]interface{})

			discountRate := vipPrivileges["discount_rate"].(float64)
			totalDiscount := 0.0

			for _, order := range orders {
				amount := order["amount"].(int)
				totalDiscount += float64(amount) * (1 - discountRate)
			}

			return map[string]interface{}{
				"total_discount": totalDiscount,
			}, nil
		},
		// 条件执行：只有当VIP特权任务成功执行时才计算优惠
		Condition: func(inputs map[string]interface{}) bool {
			_, hasPrivileges := inputs["get_vip_privileges"]
			return hasPrivileges
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
	if err := taskGraph.AddTask(getVIPPrivilegesTask); err != nil {
		fmt.Printf("Failed to add getVIPPrivilegesTask: %v\n", err)
		return
	}
	if err := taskGraph.AddTask(calculateDiscountTask); err != nil {
		fmt.Printf("Failed to add calculateDiscountTask: %v\n", err)
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
	result, err := taskGraph.Execute(ctx, graph.WithWorkerCount(3))
	if err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		return
	}

	fmt.Printf("Final result: %+v\n", result)
}
