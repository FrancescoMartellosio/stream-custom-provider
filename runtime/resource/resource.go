package resource

import (
	"context"
	"fmt"

	"sync"

	//resourcepb "github.com/nitrictech/nitric/core/pkg/proto/resources/v1"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type DockerResource string

const (
	DockerResource_Service DockerResource = "docker:service"
	DockerResource_KV      DockerResource = "docker:kv"
	DockerResource_Bucket  DockerResource = "docker:bucket"
	DockerResource_Unknown DockerResource = "docker:unknown"
)


// Resolved resource
type ResolvedResource struct {
	ContainerID string
	Name        string
	Labels      map[string]string
}

// DockerResourceResolver
type DockerResourceResolver struct {
	cacheLock sync.Mutex
	dockerCli *client.Client
	cache     map[DockerResource]map[string]ResolvedResource
}

// Ensure interface compliance
var _ = &DockerResourceResolver{}

// create instance of resolver
func NewDockerResourceResolver() (*DockerResourceResolver, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	return &DockerResourceResolver{
		dockerCli: cli,
		cache:     make(map[DockerResource]map[string]ResolvedResource),
	}, nil
}

func (d *DockerResourceResolver) populateCache(ctx context.Context) error {
	d.cacheLock.Lock()
	defer d.cacheLock.Unlock()

	if d.cache == nil {
		d.cache = make(map[DockerResource]map[string]ResolvedResource)
	}

	containers, err := d.dockerCli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list docker containers: %w", err)
	}

	for _, cont := range containers {
		// Only consider containers created by Nitric provider
		if cont.Labels["nitric-provider"] != "true" {
			continue
		}

		nameLabel := cont.Labels["nitric-resource-name"]
		typeLabel := cont.Labels["nitric-resource-type"]
		if nameLabel == "" || typeLabel == "" {
			continue
		}

		dr := DockerResource(typeLabel)
		if d.cache[dr] == nil {
			d.cache[dr] = make(map[string]ResolvedResource)
		}

		if _, exists := d.cache[dr][nameLabel]; exists {
			return fmt.Errorf("duplicate resource name found: %s of type %s", nameLabel, dr)
		}

		d.cache[dr][nameLabel] = ResolvedResource{
			ContainerID: cont.ID,
			Name:        nameLabel,
			Labels:      cont.Labels,
		}
	}

	return nil
}

// ------------------- Resource resolution -------------------
func (d *DockerResourceResolver) GetResources(ctx context.Context, typ DockerResource) (map[string]ResolvedResource, error) {
	if err := d.populateCache(ctx); err != nil {
		return nil, fmt.Errorf("error populating docker resource cache: %w", err)
	}
	return d.cache[typ], nil
}

