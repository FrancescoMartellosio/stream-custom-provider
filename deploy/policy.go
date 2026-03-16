package deploy

import (
	deploymentspb "github.com/nitrictech/nitric/core/pkg/proto/deployments/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"

)

func (a *NitricCustomPulumiProvider) Policy(ctx *pulumi.Context, parent pulumi.Resource, name string, config *deploymentspb.Policy) error {
	// TODO: Implement Policy deployment for the custom provider
	return nil
}
