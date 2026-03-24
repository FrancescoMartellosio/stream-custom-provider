package topic

import (
	"context"
	"encoding/json"
	"os"

	topicpb "github.com/nitrictech/nitric/core/pkg/proto/topics/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

type TopicsServer struct {
	kafkaAddr string
}

var _ topicpb.TopicsServer = &TopicsServer{}

// Updates a secret, creating a new one if it doesn't already exist
func (srv *TopicsServer) Publish(ctx context.Context, req *topicpb.TopicPublishRequest) (*topicpb.TopicPublishResponse, error) {
	// 1. Extract the payload from the Protobuf Struct
	payload := req.GetMessage().GetStructPayload().AsMap()
	
	// 2. Serialize map to JSON bytes
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal payload: %v", err)
	}

	// 3. Initialize a Writer for this specific topic
	// Note: In production, you'd cache these writers, but for a thesis simulation, 
	// this is fine for clarity.
	w := &kafka.Writer{
		Addr:     kafka.TCP(srv.kafkaAddr),
		Topic:    req.TopicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	// 4. Write the message to Kafka
	err = w.WriteMessages(ctx, kafka.Message{
		Value: jsonBytes,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to write to kafka: %v", err)
	}

	return &topicpb.TopicPublishResponse{}, nil
}

func New() (*TopicsServer, error) {
	// Get the Kafka address from env var, defaulting to your Master VM
	kafkaAddr := os.Getenv("KAFKA_ADDRESS")
	if kafkaAddr == "" {
		kafkaAddr = "192.168.1.23:9092" 
	}
	return &TopicsServer{
		kafkaAddr: kafkaAddr,
	}, nil
}
