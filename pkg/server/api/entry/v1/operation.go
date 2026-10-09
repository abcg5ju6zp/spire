package entry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"regexp"
	"time"

	"github.com/sirupsen/logrus"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	commonapi "github.com/spiffe/spire/pkg/common/api"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/spiffe/spire/pkg/server/api/rpccontext"
	"github.com/spiffe/spire/pkg/server/datastore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// operationIDMetadataKey is the gRPC metadata header that clients use to
	// attach a recoverable identity to a batch Entry request. When the header
	// is absent, the batch RPC behaves exactly as it did before recoverable
	// operations existed.
	operationIDMetadataKey = "x-spire-operation-id"

	// operationIDMaxLength restricts the size of client supplied operation
	// IDs.
	operationIDMaxLength = 128

	// operationTTL bounds how long the durable record of a recoverable batch
	// operation is retained. After it expires, retrying the request with the
	// same operation ID starts a new operation.
	operationTTL = 24 * time.Hour
)

// operationIDPattern matches the operation IDs accepted on batch Entry RPCs.
var operationIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// operationIDFromContext returns the recoverable operation ID attached to the
// request, if any. A missing value returns an empty ID and no error. A malformed
// value returns an InvalidArgument error.
func operationIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}

	values := md.Get(operationIDMetadataKey)
	switch len(values) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", status.Error(codes.InvalidArgument, "operation ID must be specified exactly once")
	}

	opID := values[0]
	if len(opID) == 0 || len(opID) > operationIDMaxLength || !operationIDPattern.MatchString(opID) {
		return "", status.Errorf(codes.InvalidArgument, "invalid operation ID: must be 1-%d characters long and contain only letters, digits, '.', '_' or '-'", operationIDMaxLength)
	}

	return opID, nil
}

// beginEntryBatchOperation starts a new recoverable batch operation or
// resumes an existing one.
//
// When the operation ID is new, its identity is persisted along with a
// fingerprint of the request input and the number of items. When the
// operation ID already exists, the stored fingerprint must match the current
// request; reusing an operation ID for different input is explicitly rejected
// with InvalidArgument. Replaying the same input resumes the operation.
func (s *Service) beginEntryBatchOperation(ctx context.Context, opType datastore.EntryBatchOperationType, requestHash []byte, itemCount int, opID string) error {
	log := rpccontext.Logger(ctx)

	op := &datastore.EntryBatchOperation{
		ID:          opID,
		Type:        opType,
		RequestHash: requestHash,
		ItemCount:   itemCount,
		ExpiresAt:   time.Now().Add(operationTTL),
	}

	err := s.ds.CreateEntryBatchOperation(ctx, op)
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return commonapi.MakeErr(log, codes.Internal, "failed to persist batch operation", err)
	}

	existing, err := s.ds.FetchEntryBatchOperation(ctx, opID)
	if err != nil {
		return commonapi.MakeErr(log, codes.Internal, "failed to resume batch operation", err)
	}

	if existing.Type != opType ||
		existing.ItemCount != itemCount ||
		!bytes.Equal(existing.RequestHash, requestHash) {
		return commonapi.MakeErr(log, codes.InvalidArgument, "operation ID is already in use for a different request", nil)
	}

	return nil
}

// annotateEntryBatchOperation records the operation ID in the RPC audit fields
// and echoes it back to the client in the response metadata so that all
// attempts and responses of the same operation can be correlated.
func annotateEntryBatchOperation(ctx context.Context, opID string) {
	rpccontext.AddRPCAuditFields(ctx, logrus.Fields{
		telemetry.OperationID: opID,
	})
	// The header can no longer be sent once the handler returns; any failure
	// to set it is non-fatal.
	_ = grpc.SetHeader(ctx, metadata.Pairs(operationIDMetadataKey, opID))
}

func batchCreateRequestFingerprint(req *entryv1.BatchCreateEntryRequest) []byte {
	return fingerprintMessage(&entryv1.BatchCreateEntryRequest{
		Entries: req.Entries,
	})
}

func batchUpdateRequestFingerprint(req *entryv1.BatchUpdateEntryRequest) []byte {
	return fingerprintMessage(&entryv1.BatchUpdateEntryRequest{
		Entries:   req.Entries,
		InputMask: req.InputMask,
	})
}

func batchDeleteRequestFingerprint(req *entryv1.BatchDeleteEntryRequest) []byte {
	return fingerprintMessage(&entryv1.BatchDeleteEntryRequest{
		Ids: req.Ids,
	})
}

func fingerprintMessage(m proto.Message) []byte {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		// The fingerprinted messages are fully controlled by the server and
		// are always marshalable.
		panic(err)
	}
	sum := sha256.Sum256(data)
	return sum[:]
}
