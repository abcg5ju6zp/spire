package datastore

import (
	"time"

	"github.com/spiffe/spire/proto/spire/common"
)

// EntryBatchOperationType identifies which batch Entry RPC a recoverable
// batch operation was started by.
type EntryBatchOperationType int32

const (
	_ EntryBatchOperationType = iota
	// BatchCreateEntryOperation is a BatchCreateEntry operation.
	BatchCreateEntryOperation
	// BatchUpdateEntryOperation is a BatchUpdateEntry operation.
	BatchUpdateEntryOperation
	// BatchDeleteEntryOperation is a BatchDeleteEntry operation.
	BatchDeleteEntryOperation
)

// String returns a stable, human readable name for the operation type. The
// name is persisted, so it must not change between releases.
func (t EntryBatchOperationType) String() string {
	switch t {
	case BatchCreateEntryOperation:
		return "batch_create_entry"
	case BatchUpdateEntryOperation:
		return "batch_update_entry"
	case BatchDeleteEntryOperation:
		return "batch_delete_entry"
	default:
		return "unknown"
	}
}

// EntryBatchItemOutcome classifies the durable outcome of a single item in a
// recoverable batch operation.
type EntryBatchItemOutcome int32

const (
	_ EntryBatchItemOutcome = iota
	// BatchItemCommitted means the item's mutation was applied exactly once.
	// Entry holds the resulting registration entry when one is part of the
	// response (create and update); it is nil for deletes.
	BatchItemCommitted
	// BatchItemExisted means create found a similar registration entry that
	// already existed; nothing was mutated. Entry holds the existing entry.
	BatchItemExisted
	// BatchItemNotFound means an update or delete targeted an entry that did
	// not exist; nothing was mutated.
	BatchItemNotFound
)

// EntryBatchOperation is the persisted identity of a recoverable batch Entry
// request. It records the operation ID supplied by the client, the RPC it was
// started for, a fingerprint of the request input, and the number of items in
// the request.
type EntryBatchOperation struct {
	ID          string
	Type        EntryBatchOperationType
	RequestHash []byte
	ItemCount   int
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

// EntryBatchOperationItem is the durable, per-item result of a recoverable
// batch operation. The item record is committed in the same database
// transaction as the mutation it describes (when a mutation took place).
type EntryBatchOperationItem struct {
	OperationID string
	ItemIndex   int
	Outcome     EntryBatchItemOutcome
	// Entry holds the canonical registration entry resulting from the item.
	// It is set for committed and "already existed" create and update items.
	Entry *common.RegistrationEntry
}
