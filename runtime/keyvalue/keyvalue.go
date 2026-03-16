package keyvalue

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"

	"github.com/nitrictech/nitric-provider-template/custom-provider/runtime/resource"
	kvstorepb "github.com/nitrictech/nitric/core/pkg/proto/kvstore/v1"
	"google.golang.org/protobuf/types/known/structpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RedisStore struct {
	Name    string
	Address string
	Client  *redis.Client
}

type KvStoreServer struct {
	stores map[string]*RedisStore
}

var _ kvstorepb.KvStoreServer = &KvStoreServer{}

// Updates a secret, creating a new one if it doesn't already exist
// Get an existing document
// Get an existing document
func (s *KvStoreServer) GetValue(ctx context.Context, req *kvstorepb.KvStoreGetValueRequest) (*kvstorepb.KvStoreGetValueResponse, error) {
	if s == nil {
		log.Println("Error: s is nil in GetValue")
		return nil, status.Error(codes.Internal, "kvserver not initialized")
	}
	if s.stores == nil {
		log.Println("Error: KvStoreServer.stores is nil in GetValue")
		return nil, status.Error(codes.Internal, "server stores not initialized")
	}

	store, ok := s.stores[req.Ref.Store]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "store %s not found", req.Ref.Store)
	}

	valMap, err := store.Client.HGetAll(ctx, req.Ref.Key).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis HGETALL error: %v", err)
	}

	// HGetAll returns an empty map if the key doesn't exist. Let's verify.
	if len(valMap) == 0 {
		exists, err := store.Client.Exists(ctx, req.Ref.Key).Result()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "redis EXISTS error: %v", err)
		}
		if exists == 0 {
			return nil, status.Errorf(codes.NotFound, "key %s not found", req.Ref.Key)
		}
	}

	// Convert map[string]string (from Redis) to map[string]interface{} (for structpb)
	data := make(map[string]interface{})
	for k, v := range valMap {
		data[k] = v // They come out of Redis as strings
	}

	structData, err := structpb.NewStruct(data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "structpb conversion error: %v", err)
	}

	return &kvstorepb.KvStoreGetValueResponse{
		Value: &kvstorepb.Value{
			Ref:     req.Ref,
			Content: structData,
		},
	}, nil
}

// Create a new or overwrite an existing document
func (s *KvStoreServer) SetValue(ctx context.Context, req *kvstorepb.KvStoreSetValueRequest) (*kvstorepb.KvStoreSetValueResponse, error) {
	if s == nil {
		log.Println("Error: s is nil in SetValue")
		return nil, status.Error(codes.Internal, "kvserver not initialized")
	}
	if s.stores == nil {
		log.Println("Error: KvStoreServer.stores is nil in SetValue")
		return nil, status.Error(codes.Internal, "server stores not initialized")
	}
	store, ok := s.stores[req.Ref.Store]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "store %s not found", req.Ref.Store)
	}
	dataMap := req.Content.AsMap()
	if len(dataMap) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot save an empty document")
	}

	if err := store.Client.HSet(ctx, req.Ref.Key, dataMap).Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "redis HSET error: %v", err)
	}

	return &kvstorepb.KvStoreSetValueResponse{}, nil
}

// Delete an existing document
func (*KvStoreServer) DeleteKey(context.Context, *kvstorepb.KvStoreDeleteKeyRequest) (*kvstorepb.KvStoreDeleteKeyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Unimplemented")
}

// Iterate over all keys in a store
func (*KvStoreServer) ScanKeys(*kvstorepb.KvStoreScanKeysRequest, kvstorepb.KvStore_ScanKeysServer) error {
	return status.New(codes.Unimplemented, "Unimplemented").Err()
}

func New(resolver *resource.DockerResourceResolver) (*KvStoreServer, error) {
	ctx := context.Background()

	// Get keyvaluestore containers
	resources, err := resolver.GetResources(ctx, resource.DockerResource_KV)
	if resources == nil {
		log.Println("GetResources returns nil")
		return nil, status.Error(codes.Internal, "GETRESOURCES NIL")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to resolve redis kvstore containers: %w", err)
	}

	stores := make(map[string]*RedisStore)

	for name := range resources {
		// container name = <name>-redis
		address := fmt.Sprintf("%s-redis:6379", name)

		client := redis.NewClient(&redis.Options{
			Addr: address,
		})

		// Validate connection
		if _, err := client.Ping(ctx).Result(); err != nil {
			return nil, fmt.Errorf("failed to connect to redis for %s at %s: %w",
				name, address, err)
		}

		stores[name] = &RedisStore{
			Name:    name,
			Address: address,
			Client:  client,
		}
	}

	return &KvStoreServer{stores: stores}, nil
}
