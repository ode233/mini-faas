package main

import (
	pb "aliyun/serverless/mini-faas/apiserver/proto"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"encoding/json"
	"sync"

	"google.golang.org/grpc"

	"context"
	"flag"
	"fmt"
	"time"
)

type input struct {
	functionName string
	param        string
}

// 这个参数不是时间，不能乱填
var sampleEvents = map[string]string{
	"dev_function_1": "1.2",
	"dev_function_2": "",
	"dev_function_3": "",
	"dev_function_4": "50",
	"dev_function_5": "",
}

var wg sync.WaitGroup

func main() {
	var apiserverEndpointPtr = flag.String("apiserverEndpoint", "0.0.0.0:10500", "endpoint of the apiserver")
	var functionNameStr = flag.String("functionName", "", "function name")
	var eventStr = flag.String("event", "hello, cloud native!", "function input event")
	flag.Parse()

	conn, err := grpc.Dial(*apiserverEndpointPtr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	asClient := pb.NewAPIServerClient(conn)

	lfReply, err := asClient.ListFunctions(context.Background(), &pb.ListFunctionsRequest{})
	if err != nil {
		panic(err)
	}

	if *functionNameStr != "" {
		invokeFunction(asClient, *functionNameStr, []byte(*eventStr))
		return
	}

	data, _ := json.MarshalIndent(lfReply.Functions, "", "    ")
	logger.Infof("lfReply.Functions:\n%s", data)

	for i := 0; i < 200; i++ {
		for _, f := range lfReply.Functions {
			e := sampleEvents[f.FunctionName]
			event := fmt.Sprintf(`{"functionName": "%s", "param": "%s"}`, f.FunctionName, e)
			for j := 0; j < 10; j++ {
				//invokeFunction(asClient, f.FunctionName, []byte(event))
				go invokeFunction(asClient, f.FunctionName, []byte(event))
				wg.Add(1)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	wg.Wait()
	logger.Infof("finish invoke")
	select {}
}

func invokeFunction(asClient pb.APIServerClient, functionName string, event []byte) {
	defer wg.Done()
	logger.Infof("Invoking function %s with event %s\n", functionName, string(event))
	now := time.Now().UnixNano()
	// 本地30s测2500次会有大概100多次超时，调高一些
	ctxAc, cancelAc := context.WithTimeout(context.Background(), 60*time.Second)
	invkReply, err := asClient.InvokeFunction(ctxAc, &pb.InvokeFunctionRequest{
		AccountId:    "test-account-id",
		FunctionName: functionName,
		Event:        event,
	})
	cancelAc()
	latency := (time.Now().UnixNano() - now) / 1e6
	if err != nil {
		requestId := ""
		if invkReply != nil {
			requestId = invkReply.RequestId
		}
		logger.Infof("request id: %s, Failed to invoke function %s due to %+v, latency: %d\n", requestId, functionName, err, latency)
		return
	}

	logger.Infof("request id: %s, Invoke function reply %+v, latency: %d\n", invkReply.RequestId, invkReply, latency)
}
