package deploy

import (
	"fmt"

	deploymentspb "github.com/nitrictech/nitric/core/pkg/proto/deployments/v1"
	"github.com/pulumi/pulumi-docker/sdk/v4/go/docker"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	redisImage = "redis:7"
	redisPort  = 6379
)

func (n *NitricCustomPulumiProvider) KeyValueStore(ctx *pulumi.Context, parent pulumi.Resource, name string, config *deploymentspb.KeyValueStore) error {
	// 1. Safety Checks
	if n.config == nil {
		return status.Error(codes.Internal, "provider config not initialized")
	}
	if len(n.config.Hosts) == 0 {
		return status.Error(codes.InvalidArgument, "no hosts defined in nitric.custom.yaml")
	}
	if n.network == nil {
		return status.Error(codes.Internal, "docker network not initialized in Pre()")
	}

	// 2. Define Master Node Details (Host 0)
	masterHost := n.config.Hosts[0]
	if masterHost.Port == 0 {
		masterHost.Port = 22
	}

	sshHostURL := fmt.Sprintf("ssh://%s@%s:%d", masterHost.User, masterHost.Host, masterHost.Port)

	// 3. Create a scoped Docker Provider for this Redis instance
	dockerProvider, err := docker.NewProvider(ctx, name+"-docker-ssh", &docker.ProviderArgs{
		Host: pulumi.String(sshHostURL),
		SshOpts: pulumi.StringArray{
			pulumi.String("-i"),
			n.SSHKey, // Use the pre-read key from the struct
			pulumi.String("-o"),
			pulumi.String("StrictHostKeyChecking=no"),
		},
	}, pulumi.Parent(parent))
	if err != nil {
		return fmt.Errorf("failed to create docker SSH provider: %w", err)
	}

	// 4. Pull Redis Image to the Master VM
	image, err := docker.NewRemoteImage(ctx, name+"-redis-image", &docker.RemoteImageArgs{
		Name: pulumi.String(redisImage),
	}, pulumi.Provider(dockerProvider), pulumi.Parent(parent))
	if err != nil {
		return fmt.Errorf("failed to pull Redis image: %w", err)
	}

	// 5. Deploy the Redis Container
	_, err = docker.NewContainer(ctx, name+"-redis-container", &docker.ContainerArgs{
		Image: image.ImageId,
		Name:  pulumi.String(name + "-redis"),
		Labels: docker.ContainerLabelArray{
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-provider"),
				Value: pulumi.String("true"),
			},
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-resource-type"),
				Value: pulumi.String("docker:kv"),
			},
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-resource-name"),
				Value: pulumi.String(name),
			},
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-project"),
				Value: pulumi.String(ctx.Project()),
			},
		},
		// We use the bridge network created in Pre()
		NetworksAdvanced: docker.ContainerNetworksAdvancedArray{
			&docker.ContainerNetworksAdvancedArgs{
				Name: n.network.Name,
			},
		},
		// Map the port so external services can still hit the Master IP:6379
		Ports: docker.ContainerPortArray{
			&docker.ContainerPortArgs{
				Internal: pulumi.Int(redisPort),
				External: pulumi.Int(redisPort),
			},
		},
		Restart: pulumi.String("always"),
	}, pulumi.Provider(dockerProvider), pulumi.Parent(parent))
	if err != nil {
		return fmt.Errorf("failed to create Redis container: %w", err)
	}

	// Export the address using the Master Host IP
	addr := fmt.Sprintf("%s:%d", masterHost.Host, redisPort)
	ctx.Export("kvstore_"+name+"_redis_addr", pulumi.String(addr))

	return nil
}