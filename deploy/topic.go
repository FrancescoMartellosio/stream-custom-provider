package deploy

import (
	deploymentspb "github.com/nitrictech/nitric/core/pkg/proto/deployments/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi-docker/sdk/v3/go/docker"
	"github.com/pulumi/pulumi-command/sdk/go/command/remote"
	"fmt"
)

func (a *NitricCustomPulumiProvider) Topic(ctx *pulumi.Context, parent pulumi.Resource, name string, config *deploymentspb.Topic) error {
	master := a.config.Hosts[0]
	masterConn := a.Connections[0]

	// 1. The Singleton Kafka Container
	// By using a STATIC name ("kafka-cluster-broker") instead of the 'name' variable,
	// Pulumi ensures only one container is ever managed, regardless of how many topics exist.
	kafkaBroker, err := docker.NewContainer(ctx, "kafka-cluster-broker", &docker.ContainerArgs{
		Image: pulumi.String("apache/kafka:latest"),
		Name:  pulumi.String("kafka-broker"),
		NetworksAdvanced: docker.ContainerNetworksAdvancedArray{
			&docker.ContainerNetworksAdvancedArgs{
				Name: a.network.Name,
			},
		},
		Envs: pulumi.StringArray{
			pulumi.String("KAFKA_NODE_ID=1"),
			pulumi.String("KAFKA_PROCESS_ROLES=broker,controller"),
			pulumi.String("KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093"),
			pulumi.String(fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:9092", master.Host)),
			pulumi.String("KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER"),
			pulumi.String("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
			pulumi.String("KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093"),
			pulumi.String("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"),
		},
		Ports: docker.ContainerPortArray{
			&docker.ContainerPortArgs{
				Internal: pulumi.Int(9092),
				External: pulumi.Int(9092),
			},
		},
	}, pulumi.Provider(a.DockerProviders[0]))
	if err != nil {
		return err
	}

	// 2. The Individual Topic (runs for every call)
	// Notice we use the 'name' parameter here so every Nitric topic gets its own Kafka topic.
	_, err = remote.NewCommand(ctx, fmt.Sprintf("create-kafka-topic-%s", name), &remote.CommandArgs{
		Connection: masterConn,
		// We use --if-not-exists so the command is idempotent
		Create: pulumi.Sprintf(
			"docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic %s --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1",
			name,
		),
	}, pulumi.DependsOn([]pulumi.Resource{kafkaBroker}))

	return err
}
