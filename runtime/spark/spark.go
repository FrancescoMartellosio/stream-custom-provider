package spark

import (
	"bytes"
	"context"
	"google.golang.org/protobuf/encoding/protojson"
	"fmt"
	"log"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	sparkpb "github.com/nitrictech/nitric/core/pkg/proto/spark/v1"
)

type SparkServer struct {
	sparkpb.UnimplementedSparkServer
}

func New() (*SparkServer, error) {
	return &SparkServer{}, nil
}

func (s *SparkServer) Execute(ctx context.Context, req *sparkpb.SparkExecuteRequest) (*sparkpb.SparkExecuteResponse, error) {
	log.Printf("[Spark Runtime] launching stream spark: %s ", req.GetClusterName())

	log.Printf("[DEBUG-GO] Instructions from Request: %+v", req.GetInstructions())

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.41"),)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}
	defer cli.Close()

	marshaller := protojson.MarshalOptions{
		UseProtoNames:  true,
		EmitUnpopulated: false,
	}

	jsonData, _ := marshaller.Marshal(req)
	cleanJson := strings.ReplaceAll(string(jsonData), "\n", "")

	log.Printf("[DEBUG-GO] Submitting Spark Job for Topic: %s", req.GetTablePattern())
	log.Printf("[DEBUG-GO] JSON Payload: %s", string(cleanJson))

	masterUrl := "spark://192.168.1.139:7077"
	driverHost := "192.168.1.139" 
	
	// Construct the command as a slice
	cmd := []string{
		"/opt/spark/bin/spark-submit",
		"--master", masterUrl,
		"--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
		"--conf", fmt.Sprintf("spark.driver.host=%s", driverHost),
		"--conf", "spark.driver.bindAddress=0.0.0.0",
		"/opt/spark/scripts/interpreter.py",
		cleanJson,
	}

	log.Printf("[Spark Runtime] Submitting to spark-master container...")
	go func() {
		output, err := s.runExec(context.Background(), cli, "spark-master", cmd)
		if err != nil {
			log.Printf("[Spark Runtime] Stream Job exited/failed: %v", err)
		}
		log.Printf("[Spark Runtime] Stream Output: %s", output)
	}()
	

	return &sparkpb.SparkExecuteResponse{Value: 1.0, Error: ""}, nil
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

