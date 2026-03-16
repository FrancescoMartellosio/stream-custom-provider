package spark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	sparkpb "github.com/nitrictech/nitric/core/pkg/proto/spark/v1"
)

type InterpreterPayload struct {
	Table        string                     `json:"table"`
	Instructions []*sparkpb.SparkInstruction `json:"instructions"`
}

type SparkServer struct {
	sparkpb.UnimplementedSparkServer
}

func New() (*SparkServer, error) {
	return &SparkServer{}, nil
}

func (s *SparkServer) Execute(ctx context.Context, req *sparkpb.SparkExecuteRequest) (*sparkpb.SparkExecuteResponse, error) {
	log.Printf("[Spark Runtime] Execute: %s (Pattern: %s)", req.GetClusterName(), req.GetTablePattern())

	log.Printf("[DEBUG-GO] Instructions from Request: %+v", req.GetInstructions())

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.41"),)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}
	defer cli.Close()

	payload := InterpreterPayload{
		Table:        req.GetTablePattern(),
		Instructions: req.GetInstructions(),
	}
	jsonData, _ := json.Marshal(payload)
	cleanJson := strings.ReplaceAll(string(jsonData), "\n", "")

	log.Printf("[DEBUG-GO] Marshaled JSON Payload: %s", string(cleanJson))

	masterUrl := "spark://192.168.1.133:7077"
	driverHost := "192.168.1.133" 
	redisHost := "192.168.1.133"
	jarPath := "/opt/spark/jars/spark-redis_2.12-3.1.0.jar,/opt/spark/jars/jedis-3.9.0.jar,/opt/spark/jars/commons-pool2-2.11.1.jar"

	// Construct the command as a slice
	cmd := []string{
		"/opt/spark/bin/spark-submit",
		"--master", masterUrl,
		"--jars", jarPath,
		"--conf", fmt.Sprintf("spark.driver.host=%s", driverHost),
		"--conf", "spark.driver.bindAddress=0.0.0.0",
		"--conf", fmt.Sprintf("spark.redis.host=%s", redisHost),
		"/opt/spark/scripts/interpreter.py",
		cleanJson, // No extra quotes needed here if passed as a slice element
	}

	log.Printf("[Spark Runtime] Submitting to spark-master container...")
	output, err := s.runExec(ctx, cli, "spark-master", cmd)
	if err != nil {
		log.Printf("[Spark Runtime] Job Failed: %v", err)
		return &sparkpb.SparkExecuteResponse{Value: 0, Error: err.Error()}, nil
	}

	re := regexp.MustCompile(`RESULT_START:([\d.]+):RESULT_END`)
	matches := re.FindStringSubmatch(output)
	if len(matches) > 1 {
		val, _ := strconv.ParseFloat(matches[1], 64)
		log.Printf("[Spark Runtime] Success. Result: %f", val)
		return &sparkpb.SparkExecuteResponse{Value: val}, nil
	}

	return &sparkpb.SparkExecuteResponse{Value: 0, Error: "Missing result markers in output"}, nil
}

func (s *SparkServer) runExec(ctx context.Context, cli *client.Client, containerName string, cmd []string) (string, error) {
	// Create the execution instance
	execConfig := types.ExecConfig{
		User:         "root",
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          cmd,
	}
	execCreate, err := cli.ContainerExecCreate(ctx, containerName, execConfig)
	if err != nil {
		return "", fmt.Errorf("exec create failed: %w", err)
	}

	resp, err := cli.ContainerExecAttach(ctx, execCreate.ID, types.ExecStartCheck{})
	if err != nil {
		return "", fmt.Errorf("exec attach failed: %w", err)
	}
	defer resp.Close()

	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)
	go func() {
		_, err := stdcopy.StdCopy(&outBuf, &errBuf, resp.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return "", err
		}
	case <-ctx.Done():
		return "", ctx.Err()
	}

	inspect, err := cli.ContainerExecInspect(ctx, execCreate.ID)
	if err != nil || inspect.ExitCode != 0 {
		return "", fmt.Errorf("exec failed (exit %d): %s", inspect.ExitCode, errBuf.String())
	}

	return outBuf.String(), nil
}

func (s *SparkServer) Submit(ctx context.Context, req *sparkpb.SparkSubmitRequest) (*sparkpb.SparkSubmitResponse, error) {
	return &sparkpb.SparkSubmitResponse{
		JobId:  fmt.Sprintf("job-%d", time.Now().Unix()),
		Status: "SUBMITTED",
	}, nil
}