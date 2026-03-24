package deploy

import (
	
	"os"
	"fmt"
	"strings"

	_ "embed"

	"github.com/nitrictech/nitric/cloud/common/deploy"
	"github.com/nitrictech/nitric/cloud/common/deploy/provider"
	"github.com/nitrictech/nitric/cloud/common/deploy/pulumix"
	"github.com/pulumi/pulumi-docker/sdk/v4/go/docker"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi-command/sdk/go/command/remote"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NitricCustomPulumiProvider struct {
    *deploy.CommonStackDetails
    StackId string
    config  *CustomConfig
    network *docker.Network

    
    SSHKey pulumi.String
    Connections map[int]*remote.ConnectionArgs
    DockerProviders map[int]*docker.Provider

    provider.NitricDefaultOrder
}

var _ provider.NitricPulumiProvider = (*NitricCustomPulumiProvider)(nil)

func (a *NitricCustomPulumiProvider) Config() (auto.ConfigMap, error) {
	return auto.ConfigMap{
		"docker:version": auto.ConfigValue{Value: deploy.PulumiDockerVersion},
	}, nil
}

func (a *NitricCustomPulumiProvider) Init(attributes map[string]interface{}) error {
	var err error

	a.CommonStackDetails, err = deploy.CommonStackDetailsFromAttributes(attributes)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}

	a.config, err = ConfigFromAttributes(attributes)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad stack configuration: %s", err)
	}

	return nil
}

func (a *NitricCustomPulumiProvider) Pre(ctx *pulumi.Context, resources []*pulumix.NitricPulumiResource[any]) error {
    if a.config == nil {
        return fmt.Errorf("provider config was not initialized in Init")
    }

    // 2. Initialize the connections map
    a.Connections = make(map[int]*remote.ConnectionArgs)

    // 3. Read the SSH key once
    sshKeyData, err := os.ReadFile(a.config.SSHKey)
    if err != nil {
        return fmt.Errorf("failed to read sshKey: %w", err)
    }
    a.SSHKey = pulumi.String(string(sshKeyData)) // Fixed assignment

    // 4. Bootstrap hosts
    for i, host := range a.config.Hosts {
        if host.Port == 0 { host.Port = 22 }
        conn := &remote.ConnectionArgs{
            Host:       pulumi.String(host.Host),
            User:       pulumi.String(host.User),
            Port:       pulumi.Float64(host.Port),
            PrivateKey: a.SSHKey,
        }
        a.Connections[i] = conn

        _, err = remote.NewCommand(ctx, fmt.Sprintf("bootstrap-host-%d", i), &remote.CommandArgs{
            Connection: conn,
            Create: pulumi.String("if ! command -v docker >/dev/null; then curl -fsSL https://get.docker.com | sh; fi"),
        })
        if err != nil { return err }
    }

    // 5. Create the Master Docker Provider & Network
    master := a.config.Hosts[0]
    masterDockerProv, err := docker.NewProvider(ctx, "master-docker-prov", &docker.ProviderArgs{
        Host: pulumi.Sprintf("ssh://%s@%s:%d", master.User, master.Host, master.Port),
        SshOpts: pulumi.StringArray{
            pulumi.String("-i"), pulumi.String(a.config.SSHKey),
            pulumi.String("-o"), pulumi.String("StrictHostKeyChecking=no"),
        },
    })
    if err != nil { return err }

    if a.DockerProviders == nil {
        a.DockerProviders = make(map[int]*docker.Provider)
    }
    a.DockerProviders[0] = masterDockerProv

    // This assigns the actual Pulumi resource to the struct to prevent the nil panic
    a.network, err = docker.NewNetwork(ctx, "nitric-net", &docker.NetworkArgs{
        Name:   pulumi.String("nitric-net"),
        Driver: pulumi.String("bridge"),
    }, pulumi.Provider(masterDockerProv))

    return err
}

func (a *NitricCustomPulumiProvider) Post(ctx *pulumi.Context) error {
	return nil
}

func (a *NitricCustomPulumiProvider) Result(ctx *pulumi.Context) (pulumi.StringOutput, error) {
	outputs := []interface{}{}

	output, ok := pulumi.All(outputs...).ApplyT(func(details []interface{}) string {
		stringyOutputs := make([]string, len(details))
		for i, d := range details {
			stringyOutputs[i] = d.(string)
		}

		return strings.Join(stringyOutputs, "\n")
	}).(pulumi.StringOutput)

	if !ok {
		return pulumi.StringOutput{}, fmt.Errorf("failed to generate pulumi output")
	}

	return output, nil
}

func NewNitricCustomPulumiProvider() *NitricCustomPulumiProvider {
	return &NitricCustomPulumiProvider{}
}