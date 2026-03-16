package deploy

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"

	deploymentspb "github.com/nitrictech/nitric/core/pkg/proto/deployments/v1"
	"github.com/pulumi/pulumi-command/sdk/go/command/remote"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

//go:embed interpreter.py
var interpreterFiles embed.FS

const sparkImage = "apache/spark:3.5.0"

func (n *NitricCustomPulumiProvider) Spark(ctx *pulumi.Context, parent pulumi.Resource, name string, config *deploymentspb.Spark) error {
	if n.Connections == nil || len(n.Connections) == 0 {
		return fmt.Errorf("provider connections not initialized in Pre")
	}

	masterConn := n.Connections[0]

	const remoteJarDir = "/home/nitric/spark-jars"

	// List of all required JARs for Spark-Redis to actually work
	jars := []string{
		"spark-redis_2.12-3.1.0.jar",
		"jedis-3.9.0.jar",
		"commons-pool2-2.11.1.jar",
	}

	//prepare the jars for the spark redis connector on each host
	var prepResources []pulumi.Resource
	for i, host := range n.Connections {
		prepCmd, err := remote.NewCommand(ctx, fmt.Sprintf("%s-prep-host-%d", name, i), &remote.CommandArgs{
			Connection: host,
			Create: pulumi.Sprintf(`
				mkdir -p %s
				cd %s
				if [ ! -f spark-redis_2.12-3.1.0.jar ]; then
				  curl -O -L https://repo1.maven.org/maven2/com/redislabs/spark-redis_2.12/3.1.0/spark-redis_2.12-3.1.0.jar
				fi
				if [ ! -f jedis-3.9.0.jar ]; then
				  curl -O -L https://repo1.maven.org/maven2/redis/clients/jedis/3.9.0/jedis-3.9.0.jar
				fi
				if [ ! -f commons-pool2-2.11.1.jar ]; then
				  curl -O -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
				fi
			`, remoteJarDir, remoteJarDir),
		})
		if err != nil {
			return err
		}
		prepResources = append(prepResources, prepCmd)
	}

	// Helper to build the -v mount strings for Docker
	var jarMountFlags string
	for _, jar := range jars {
		jarMountFlags += fmt.Sprintf("-v %s/%s:/opt/spark/jars/%s ", remoteJarDir, jar, jar)
	}

	//embed the interpreter file and write it to a temp location
	tempInterpreterPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s-interpreter.py", name))
	data, err := interpreterFiles.ReadFile("interpreter.py")
	if err != nil {
		return fmt.Errorf("failed to read embedded interpreter file: %w", err)
	}
	if err := os.WriteFile(tempInterpreterPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary interpreter file: %w", err)
	}

	//upload the intepreter to the master host
	interpreterRes, err := remote.NewCopyFile(ctx, name+"-upload-interpreter", &remote.CopyFileArgs{
		Connection: masterConn,
		LocalPath:  pulumi.String(tempInterpreterPath),
		RemotePath: pulumi.String("/home/nitric/interpreter.py"),
	}, pulumi.Parent(parent), pulumi.DependsOn(prepResources))
	if err != nil {
		return err
	}

	
	startMaster, err := remote.NewCommand(ctx, name+"-spark-master", &remote.CommandArgs{
		Connection: masterConn,
		Create: pulumi.Sprintf(`
			docker rm -f spark-master || true
			docker run -d \
			--name spark-master \
			--network host \
			-e SPARK_PUBLIC_DNS=%s \
			-v /home/nitric/interpreter.py:/opt/spark/scripts/interpreter.py \
			%s \
			%s \
			/opt/spark/bin/spark-class \
			org.apache.spark.deploy.master.Master \
			--ip %s \
			--port 7077 \
			--webui-port 8080
		`, n.config.Hosts[0].Host, jarMountFlags, sparkImage, n.config.Hosts[0].Host),
	}, pulumi.DependsOn([]pulumi.Resource{interpreterRes}), pulumi.Parent(parent))
	if err != nil {
		return err
	}

	for hi, hostConn := range n.Connections {
		for wi := 0; wi < int(config.WorkersPerHost); wi++ {
			workerID := fmt.Sprintf("%s-worker-h%d-w%d", name, hi, wi)
			
			_, err := remote.NewCommand(ctx, workerID, &remote.CommandArgs{
				Connection: hostConn,
				Create: pulumi.Sprintf(`
					docker rm -f %s || true
					docker run -d \
					--name %s \
					--network host \
					-e SPARK_PUBLIC_DNS=%s \
					%s \
					%s \
					/opt/spark/bin/spark-class \
					org.apache.spark.deploy.worker.Worker \
					spark://%s:7077 \
					--ip %s \
					--cores %d \
					--memory %dg
				`,
					workerID, workerID, 
					n.config.Hosts[hi].Host, // Worker's IP for DNS
					jarMountFlags,
					sparkImage,
					n.config.Hosts[0].Host,  // Master IP
					n.config.Hosts[hi].Host, // Worker's IP for binding
					config.CpusPerWorker, config.MemoryGb,
				),
			}, pulumi.DependsOn([]pulumi.Resource{startMaster}), pulumi.Parent(parent))
			if err != nil {
				return err
			}
		}
	}

	ctx.Export("spark_master_url_"+name, pulumi.Sprintf("spark://%s:7077", n.config.Hosts[0].Host))

	return nil
}