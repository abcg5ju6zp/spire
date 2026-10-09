package sqlstore

import (
	"context"
	"time"

	"github.com/jinzhu/gorm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/sqlcommon"
	"github.com/spiffe/spire/proto/spire/common"
)

// CreateEntryBatchOperation persists the identity of a new recoverable batch
// operation.
func (ds *Plugin) CreateEntryBatchOperation(ctx context.Context, op *datastore.EntryBatchOperation) (err error) {
	return ds.withWriteTx(ctx, func(tx *gorm.DB) error {
		return createEntryBatchOperation(tx, op)
	})
}

func createEntryBatchOperation(tx *gorm.DB, op *datastore.EntryBatchOperation) error {
	model := EntryBatchOp{
		OperationID: op.ID,
		Type:        op.Type.String(),
		RequestHash: op.RequestHash,
		ItemCount:   op.ItemCount,
		ExpiresAt:   op.ExpiresAt.Unix(),
	}
	if err := tx.Create(&model).Error; err != nil {
		return sqlcommon.NewWrappedSQLError(err)
	}
	op.CreatedAt = model.CreatedAt
	return nil
}

// FetchEntryBatchOperation returns a previously persisted recoverable batch
// operation by ID.
func (ds *Plugin) FetchEntryBatchOperation(ctx context.Context, operationID string) (op *datastore.EntryBatchOperation, err error) {
	if err = ds.withReadTx(ctx, func(tx *gorm.DB) error {
		var models []EntryBatchOp
		if err := tx.Limit(1).Find(&models, "operation_id = ?", operationID).Error; err != nil {
			return sqlcommon.NewWrappedSQLError(err)
		}
		if len(models) == 0 {
			return entryBatchOpNotFoundError()
		}
		op = modelToEntryBatchOperation(&models[0])
		return nil
	}); err != nil {
		return nil, err
	}
	return op, nil
}

func modelToEntryBatchOperation(model *EntryBatchOp) *datastore.EntryBatchOperation {
	return &datastore.EntryBatchOperation{
		ID:          model.OperationID,
		Type:        entryBatchOperationTypeFromString(model.Type),
		RequestHash: model.RequestHash,
		ItemCount:   model.ItemCount,
		ExpiresAt:   time.Unix(model.ExpiresAt, 0),
		CreatedAt:   model.CreatedAt,
	}
}

func entryBatchOperationTypeFromString(s string) datastore.EntryBatchOperationType {
	switch s {
	case datastore.BatchCreateEntryOperation.String():
		return datastore.BatchCreateEntryOperation
	case datastore.BatchUpdateEntryOperation.String():
		return datastore.BatchUpdateEntryOperation
	case datastore.BatchDeleteEntryOperation.String():
		return datastore.BatchDeleteEntryOperation
	default:
		return 0
	}
}

// CreateOrReturnRegistrationEntryForBatchOperation atomically replays or
// applies a single create item of a recoverable batch operation, persisting
// the item result together with the mutation.
func (ds *Plugin) CreateOrReturnRegistrationEntryForBatchOperation(ctx context.Context,
	entry *common.RegistrationEntry, operationID string, itemIndex int,
) (registrationEntry *common.RegistrationEntry, existing bool, err error) {
	var replay *EntryBatchOpItem

	if txErr := ds.withWriteTx(ctx, func(tx *gorm.DB) error {
		item, err := fetchEntryBatchOpItem(tx, operationID, itemIndex)
		if err != nil {
			return err
		}
		if item != nil {
			replay = item
			return nil
		}

		if err := validateRegistrationEntry(entry); err != nil {
			return err
		}

		registrationEntry, err = lookupSimilarEntry(ctx, ds.db, tx, entry)
		if err != nil {
			return err
		}
		if registrationEntry != nil {
			existing = true
			return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemExisted, registrationEntry)
		}

		registrationEntry, err = createRegistrationEntry(tx, entry)
		if err != nil {
			return err
		}

		if err := createRegistrationEntryEvent(tx, &datastore.RegistrationEntryEvent{
			EntryID: registrationEntry.EntryId,
		}); err != nil {
			return err
		}

		return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemCommitted, registrationEntry)
	}); txErr != nil {
		return nil, false, txErr
	}

	if replay != nil {
		return entryBatchOpItemResult(replay)
	}
	return registrationEntry, existing, nil
}

// UpdateRegistrationEntryForBatchOperation atomically replays or applies a
// single update item of a recoverable batch operation, persisting the item
// result together with the mutation.
func (ds *Plugin) UpdateRegistrationEntryForBatchOperation(ctx context.Context,
	e *common.RegistrationEntry, mask *common.RegistrationEntryMask,
	operationID string, itemIndex int,
) (entry *common.RegistrationEntry, err error) {
	var replay *EntryBatchOpItem
	var notFound bool

	if txErr := ds.withReadModifyWriteTx(ctx, func(tx *gorm.DB) error {
		item, err := fetchEntryBatchOpItem(tx, operationID, itemIndex)
		if err != nil {
			return err
		}
		if item != nil {
			replay = item
			return nil
		}

		// Determine existence explicitly (and lock the row) before mutating so
		// that the not-found result can be journaled and committed in this
		// transaction.
		var existingEntry RegisteredEntry
		db := tx.First(&existingEntry, "entry_id = ?", e.EntryId)
		switch {
		case db.RecordNotFound():
			notFound = true
			return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemNotFound, nil)
		case db.Error != nil:
			return sqlcommon.NewWrappedSQLError(db.Error)
		}

		entry, err = updateRegistrationEntry(tx, e, mask)
		if err != nil {
			return err
		}

		if err := createRegistrationEntryEvent(tx, &datastore.RegistrationEntryEvent{
			EntryID: entry.EntryId,
		}); err != nil {
			return err
		}

		return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemCommitted, entry)
	}); txErr != nil {
		return nil, txErr
	}

	switch {
	case replay != nil:
		if replay.Outcome == int(datastore.BatchItemNotFound) {
			return nil, entryBatchItemNotFoundError()
		}
		registrationEntry, _, err := entryBatchOpItemResult(replay)
		return registrationEntry, err
	case notFound:
		return nil, entryBatchItemNotFoundError()
	default:
		return entry, nil
	}
}

// DeleteRegistrationEntryForBatchOperation atomically replays or applies a
// single delete item of a recoverable batch operation, persisting the item
// result together with the mutation.
func (ds *Plugin) DeleteRegistrationEntryForBatchOperation(ctx context.Context,
	entryID string, operationID string, itemIndex int,
) (registrationEntry *common.RegistrationEntry, err error) {
	var replay *EntryBatchOpItem
	var notFound bool

	if txErr := ds.withWriteTx(ctx, func(tx *gorm.DB) error {
		item, err := fetchEntryBatchOpItem(tx, operationID, itemIndex)
		if err != nil {
			return err
		}
		if item != nil {
			replay = item
			return nil
		}

		var existingEntry RegisteredEntry
		db := tx.First(&existingEntry, "entry_id = ?", entryID)
		switch {
		case db.RecordNotFound():
			notFound = true
			return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemNotFound, nil)
		case db.Error != nil:
			return sqlcommon.NewWrappedSQLError(db.Error)
		}

		registrationEntry, err = deleteRegistrationEntry(tx, entryID)
		if err != nil {
			return err
		}

		if err := createRegistrationEntryEvent(tx, &datastore.RegistrationEntryEvent{
			EntryID: entryID,
		}); err != nil {
			return err
		}

		return createEntryBatchOpItem(tx, operationID, itemIndex, datastore.BatchItemCommitted, registrationEntry)
	}); txErr != nil {
		return nil, txErr
	}

	switch {
	case replay != nil:
		if replay.Outcome == int(datastore.BatchItemNotFound) {
			return nil, entryBatchItemNotFoundError()
		}
		registrationEntry, _, err := entryBatchOpItemResult(replay)
		return registrationEntry, err
	case notFound:
		return nil, entryBatchItemNotFoundError()
	default:
		return registrationEntry, nil
	}
}

// PruneEntryBatchOperations removes batch operations (and their item results)
// that expired before the given time.
func (ds *Plugin) PruneEntryBatchOperations(ctx context.Context, expiredBefore time.Time) (err error) {
	return ds.withWriteTx(ctx, func(tx *gorm.DB) error {
		var expired []EntryBatchOp
		if err := tx.Where("expires_at < ?", expiredBefore.Unix()).Find(&expired).Error; err != nil {
			return sqlcommon.NewWrappedSQLError(err)
		}
		if len(expired) == 0 {
			return nil
		}

		operationIDs := make([]string, 0, len(expired))
		for _, op := range expired {
			operationIDs = append(operationIDs, op.OperationID)
		}

		if err := tx.Where("operation_id IN (?)", operationIDs).Delete(&EntryBatchOpItem{}).Error; err != nil {
			return sqlcommon.NewWrappedSQLError(err)
		}

		if err := tx.Where("operation_id IN (?)", operationIDs).Delete(&EntryBatchOp{}).Error; err != nil {
			return sqlcommon.NewWrappedSQLError(err)
		}

		return nil
	})
}

func fetchEntryBatchOpItem(tx *gorm.DB, operationID string, itemIndex int) (*EntryBatchOpItem, error) {
	// Find into a slice: gorm returns ErrRecordNotFound when a Find into a
	// struct matches no rows, but an empty slice is not an error.
	var items []EntryBatchOpItem
	if err := tx.Limit(1).Find(&items, "operation_id = ? AND item_index = ?", operationID, itemIndex).Error; err != nil {
		return nil, sqlcommon.NewWrappedSQLError(err)
	}
	if len(items) == 0 {
		return nil, nil
	}
	return &items[0], nil
}

func createEntryBatchOpItem(tx *gorm.DB, operationID string, itemIndex int, outcome datastore.EntryBatchItemOutcome, entry *common.RegistrationEntry) error {
	var entryData []byte
	if entry != nil {
		data, err := proto.Marshal(entry)
		if err != nil {
			return sqlcommon.NewWrappedSQLError(err)
		}
		entryData = data
	}

	if err := tx.Create(&EntryBatchOpItem{
		OperationID: operationID,
		ItemIndex:   itemIndex,
		Outcome:     int(outcome),
		EntryData:   entryData,
	}).Error; err != nil {
		return sqlcommon.NewWrappedSQLError(err)
	}
	return nil
}

func entryBatchOpItemResult(item *EntryBatchOpItem) (*common.RegistrationEntry, bool, error) {
	existing := item.Outcome == int(datastore.BatchItemExisted)
	if len(item.EntryData) == 0 {
		return nil, existing, nil
	}
	entry := &common.RegistrationEntry{}
	if err := proto.Unmarshal(item.EntryData, entry); err != nil {
		return nil, false, sqlcommon.NewWrappedSQLError(err)
	}
	return entry, existing, nil
}

func entryBatchOpNotFoundError() error {
	return status.Error(codes.NotFound, "entry batch operation not found")
}

func entryBatchItemNotFoundError() error {
	// Match the status produced by withTx/gormToGRPCStatus when a mutation
	// targets a missing registration entry, so replayed and live results are
	// indistinguishable to callers.
	return status.Error(codes.NotFound, "datastore-sql: record not found")
}
