package deploy

import (
	"fmt"
	"strings"
	"time"

	"github.com/nitrictech/nitric/cloud/common/deploy/provider"
	"github.com/nitrictech/nitric/cloud/common/deploy/pulumix"
	"github.com/pulumi/pulumi-docker/sdk/v4/go/docker"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// Service deploys a Nitric service to the Master VM via SSH and Docker Hub
func (n *NitricCustomPulumiProvider) Service(
	ctx *pulumi.Context,
	parent pulumi.Resource,
	name string,
	svcConfig *pulumix.NitricPulumiServiceConfig,
	runtime provider.RuntimeProvider,
) error {
	
	// Original logic for subname and image source
	name = name + "subname"
	imgSrc := svcConfig.GetImage()
	if imgSrc == nil || imgSrc.GetUri() == "" {
		return fmt.Errorf("service %q must specify a valid image source", name)
	}

	// Create a unique repository URL for Docker Hub
	repoUrl := fmt.Sprintf("docker.io/francescomartellosio/%s-new:%d", strings.ToLower(name), time.Now().Unix())

	// Wrap image with nitric runtime server
	localImg, err := NewDockerImage(ctx, name, &ImageArgs{
		SourceImage:   imgSrc.GetUri(),
		RepositoryUrl: pulumi.String(repoUrl),
		Runtime:       runtime(),
	}, pulumi.Parent(parent))
	if err != nil {
		return fmt.Errorf("failed to create Nitric image: %w", err)
	}

	// --- NEW MULTI-HOST CONFIG LOGIC ---
	if len(n.config.Hosts) == 0 {
		return fmt.Errorf("no hosts defined in nitric.custom.yaml for service deployment")
	}

	// We deploy Services to the Master host (first host in the list)
	// so they can communicate with the Spark Master/Redis on the same bridge network
	masterHost := n.config.Hosts[0]
	if masterHost.Port == 0 {
		masterHost.Port = 22
	}

	// FIXED: Use this variable in the Provider below
    sshURL := fmt.Sprintf("ssh://%s@%s:%d", masterHost.User, masterHost.Host, masterHost.Port)

    // FIXED: Removed duplicate masterHost declaration and := on existing err
    dockerProvider, err := docker.NewProvider(ctx, name+"-ssh-provider", &docker.ProviderArgs{
        Host: pulumi.String(sshURL), // Using the variable now
        SshOpts: pulumi.StringArray{
            pulumi.String("-i"), n.SSHKey, 
            pulumi.String("-o"), pulumi.String("StrictHostKeyChecking=no"),
        },
    })
	if err != nil {
		return fmt.Errorf("failed to create Docker SSH provider: %w", err)
	}

	// Prepare environment variables
	envMap := svcConfig.Env()
	envs := pulumi.StringArray{}
	for k, v := range envMap {
		envs = append(envs, pulumi.String(fmt.Sprintf("%s=%s", k, v)))
	}

	// Pull the image from Docker Hub to the remote Master VM
	remoteImg, err := docker.NewRemoteImage(ctx, name+"-remote-img", &docker.RemoteImageArgs{
		Name:        localImg.ImageName,
		KeepLocally: pulumi.Bool(false),
	}, pulumi.Provider(dockerProvider))
	if err != nil {
		return err
	}

	// Run container on the Master VM through the ssh provider
	_, err = docker.NewContainer(ctx, name+"-container", &docker.ContainerArgs{
		Name:  pulumi.String(name),
		Image: remoteImg.Name,
		Labels: docker.ContainerLabelArray{
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-provider"),
				Value: pulumi.String("true"),
			},
			docker.ContainerLabelArgs{
				Label: pulumi.String("nitric-resource-type"),
				Value: pulumi.String("service"),
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
		NetworksAdvanced: docker.ContainerNetworksAdvancedArray{
			&docker.ContainerNetworksAdvancedArgs{
				Name: n.network.Name, // Uses the network created in Pre()
			},
		},
		Mounts: docker.ContainerMountArray{
			docker.ContainerMountArgs{
				Target: pulumi.String("/var/run/docker.sock"),
				Source: pulumi.String("/var/run/docker.sock"),
				Type:   pulumi.String("bind"),
			},
		},
		Restart: pulumi.String("always"),
		Envs:    envs,
	}, pulumi.Provider(dockerProvider), pulumi.Parent(parent))
	if err != nil {
		return fmt.Errorf("failed to start container on VM: %w", err)
	}

	// Export the Master VM host address for this service
	ctx.Export(name+"_service_addr", pulumi.String(masterHost.Host))

	return nil
}