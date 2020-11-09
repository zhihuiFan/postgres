/*
 * zedstore_visibility.c
 *		Routines for MVCC in Zedstore
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_visibility.c
 */
#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undorec.h"
#include "storage/procarray.h"

static bool
zs_tuplelock_compatible(LockTupleMode mode, LockTupleMode newmode)
{
	switch (newmode)
	{
		case LockTupleKeyShare:
			return mode == LockTupleKeyShare ||
				mode == LockTupleShare ||
				mode == LockTupleNoKeyExclusive;

		case LockTupleShare:
			return mode == LockTupleKeyShare ||
				mode == LockTupleShare;

		case LockTupleNoKeyExclusive:
			return mode == LockTupleKeyShare;
		case LockTupleExclusive:
			return false;

		default:
			elog(ERROR, "unknown tuple lock mode %d", newmode);
	}
}

static bool
am_i_holding_lock(Relation rel, ZSUndoRecPtr undo_ptr,
				  ZSUndoRecPtr recent_oldest_undo)
{
	ZSUndoRec  *undorec;

	for (;;)
	{
		/* Is it visible? */
		if (undo_ptr.counter < recent_oldest_undo.counter)
			return false;

		/* have to fetch the UNDO record */
		undorec = zsundo_fetch_record(rel, undo_ptr);
		if (!undorec)
		{
			recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
			if (undo_ptr.counter >= recent_oldest_undo.counter)
				elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
					 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
			return false;
		}

		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
				return true;
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
				return true;
		}
		else if (undorec->type == ZSUNDO_TYPE_DELETE)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
				return true;
		}
		else if (undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
				return true;
		}
		undo_ptr = undorec->prevundorec;
	}
}

/*
 * Like HeapTupleSatisfiesUpdate.
 *
 * When returns TM_Ok, this also returns a flag in *undo_record_needed, to indicate
 * whether the old UNDO record is still of interest to anyone. If the old record
 * belonged to an aborted deleting transaction, for example, it can be ignored.
 *
 * This does more than HeapTupleSatisfiesUpdate. If HeapTupleSatisfiesUpdate sees
 * an updated or locked tuple, it returns TM_BeingUpdated, and the caller has to
 * check if the tuple lock is compatible with the update. zs_SatisfiesUpdate
 * checks if the new lock mode is compatible with the old one, and returns TM_Ok
 * if so. Waiting for conflicting locks is left to the caller.
 *
 * This is also used for tuple locking (e.g. SELECT FOR UPDATE). 'mode' indicates
 * the lock mode. For a genuine UPDATE, pass LockTupleExclusive or
 * LockTupleNoKeyExclusive depending on whether key columns are being modified.
 *
 * If the tuple was UPDATEd, *next_tid is set to the TID of the new row version.
 */
TM_Result
zs_SatisfiesUpdate(Relation rel, Snapshot snapshot,
				   ZSUndoRecPtr recent_oldest_undo,
				   zstid item_tid,
				   LockTupleMode mode,
				   bool *undo_record_needed, bool *this_xact_has_lock,
				   TM_FailureData *tmfd,
				   zstid *next_tid, ZSUndoSlotVisibility *visi_info)
{
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;
	int			chain_depth = 0;

	*this_xact_has_lock = false;
	*undo_record_needed = true;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:
	chain_depth++;

retry_fetch:
	/* Is it visible? */
	if (undo_ptr.counter < recent_oldest_undo.counter)
	{
		/*
		 * The old UNDO record is no longer visible to anyone, so we don't
		 * need to keep it. If this record was not the one directly referenced
		 * from the item, then we must keep it, though. For example, if there
		 * is a chain (item -> LOCK_TUPLE -> INSERT), and the INSERT record is
		 * no longer needed by anyone, we must still keep the pointer to the LOCK
		 * record.
		 */
		if (chain_depth == 1)
			*undo_record_needed = false;

		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return TM_Ok;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto retry_fetch;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		visi_info->xmin = undorec->xid;
		visi_info->cmin = undorec->cid;

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			*this_xact_has_lock = true;
			if (undorec->cid >= snapshot->curcid)
				return TM_Invisible;	/* inserted after scan started */
		}
		else if (TransactionIdIsInProgress(undorec->xid))
			return TM_Invisible;		/* inserter has not committed yet */
		else if (!TransactionIdDidCommit(undorec->xid))
		{
			/* it must have aborted or crashed */
			return TM_Invisible;
		}

		/* The tuple is visible to use. But can we lock it? */

		/*
		 * No conflict with this lock. Look at the previous UNDO record, there
		 * might be more locks.
		 *
		 * FIXME: Shouldn't we drill down to the INSERT record and check if
		 * that's visible to us first, before looking at the lockers?
		 */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
		ZSUndoRec_TupleLock *lock_undorec = (ZSUndoRec_TupleLock *) undorec;

		/*
		 * If any subtransaction of the current top transaction already holds
		 * a lock as strong as or stronger than what we're requesting, we
		 * effectively hold the desired lock already.  We *must* succeed
		 * without trying to take the tuple lock, else we will deadlock
		 * against anyone wanting to acquire a stronger lock.
		 */
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			*this_xact_has_lock = true;
			if (lock_undorec->lockmode >= mode)
			{
				*undo_record_needed = true;
				return TM_Ok;
			}
		}
		else if (!zs_tuplelock_compatible(lock_undorec->lockmode, mode) &&
				 TransactionIdIsInProgress(undorec->xid))
		{
			tmfd->ctid = ItemPointerFromZSTid(item_tid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, undorec->prevundorec, recent_oldest_undo);

			return TM_BeingModified;
		}

		/*
		 * No conflict with this lock. Look at the previous UNDO record, there
		 * might be more locks.
		 *
		 * FIXME: Shouldn't we drill down to the INSERT record and check if
		 * that's visible to us first, before looking at the lockers?
		 */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE)
	{
		ZSUndoRec_Delete *deleterec = (ZSUndoRec_Delete *) undorec;
		if (visi_info)
		{
			visi_info->xmin = undorec->xid;
			visi_info->cmin = undorec->cid;
		}

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			*this_xact_has_lock = true;
			if (undorec->cid >= snapshot->curcid)
			{
				tmfd->ctid = ItemPointerFromZSTid(item_tid);
				tmfd->xmax = undorec->xid;
				tmfd->cmax = undorec->cid;
				return TM_SelfModified;	/* deleted/updated after scan started */
			}
			else
				return TM_Invisible;	/* deleted before scan started */
		}

		if (TransactionIdIsInProgress(undorec->xid))
		{
			tmfd->ctid = ItemPointerFromZSTid(item_tid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, undorec->prevundorec, recent_oldest_undo);

			return TM_BeingModified;
		}

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter must have aborted or crashed. We have to keep following the
			 * undo chain, in case there are LOCK records that are still visible
			 */
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
		}

		tmfd->xmax = undorec->xid;
		tmfd->cmax = InvalidCommandId;
		if (deleterec->changedPart)
		{
			ItemPointerSet(&tmfd->ctid, MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber);
			*next_tid = InvalidZSTid;
			return TM_Updated;
		}
		else
		{
			tmfd->ctid = ItemPointerFromZSTid(item_tid);
			return TM_Deleted;
		}
	}
	else if (undorec->type == ZSUNDO_TYPE_UPDATE)
	{
		/* updated-away tuple */
		ZSUndoRec_Update *updaterec = (ZSUndoRec_Update *) undorec;
		LockTupleMode old_lockmode;
		if (visi_info)
		{
			visi_info->xmin = undorec->xid;
			visi_info->cmin = undorec->cid;
		}

		*next_tid = updaterec->newtid;
		old_lockmode = updaterec->key_update ? LockTupleExclusive : LockTupleNoKeyExclusive;

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			*this_xact_has_lock = true;
			if (zs_tuplelock_compatible(old_lockmode, mode))
				return TM_Ok;

			if (undorec->cid >= snapshot->curcid)
			{
				tmfd->ctid = ItemPointerFromZSTid(item_tid);
				tmfd->xmax = undorec->xid;
				tmfd->cmax = undorec->cid;
				return TM_SelfModified;	/* deleted/updated after scan started */
			}
			else
				return TM_Invisible;	/* deleted before scan started */
		}

		if (TransactionIdIsInProgress(undorec->xid))
		{
			if (zs_tuplelock_compatible(old_lockmode, mode))
				return TM_Ok;

			tmfd->ctid = ItemPointerFromZSTid(item_tid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, undorec->prevundorec, recent_oldest_undo);

			return TM_BeingModified;
		}

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter must have aborted or crashed. We have to keep following the
			 * undo chain, in case there are LOCK records that are still visible
			 */
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
		}

		if (zs_tuplelock_compatible(old_lockmode, mode))
			return TM_Ok;

		tmfd->ctid = ItemPointerFromZSTid(((ZSUndoRec_Update *) undorec)->newtid);
		tmfd->xmax = undorec->xid;
		tmfd->cmax = InvalidCommandId;
		return TM_Updated;
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
}


/*
 * Like HeapTupleSatisfiesAny
 */
static bool
zs_SatisfiesAny(ZSTidTreeScan *scan, ZSUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:
	/* If this record is "old", then the record is visible. */
	if (undo_ptr.counter < scan->recent_oldest_undo.counter)
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto fetch_undo_record;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		visi_info->xmin = undorec->xid;
		visi_info->cmin = undorec->cid;
		return true;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 undorec->type == ZSUNDO_TYPE_UPDATE ||
			 undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);

	return true;
}

/*
 * helper function to zs_SatisfiesMVCC(), to check if the given XID
 * is visible to the snapshot.
 */
static bool
xid_is_visible(Snapshot snapshot, TransactionId xid, CommandId cid, bool *aborted)
{
	*aborted = false;
	if (TransactionIdIsCurrentTransactionId(xid))
	{
		if (cid >= snapshot->curcid)
			return false;
		else
			return true;
	}
	else if (XidInMVCCSnapshot(xid, snapshot))
		return false;
	else if (TransactionIdDidCommit(xid))
	{
		return true;
	}
	else
	{
		/* it must have aborted or crashed */
		*aborted = true;
		return false;
	}
}

/*
 * Like HeapTupleSatisfiesMVCC
 */
static bool
zs_SatisfiesMVCC(ZSTidTreeScan *scan,
				 TransactionId *obsoleting_xid, zstid *next_tid,
				 ZSUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;
	bool		aborted;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:
	/* If this record is "old", then the record is visible. */
	if (undo_ptr.counter < scan->recent_oldest_undo.counter)
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto fetch_undo_record;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		/* Inserted tuple */
		bool		result;
		result = xid_is_visible(snapshot, undorec->xid, undorec->cid, &aborted);
		if (!result && !aborted)
			*obsoleting_xid = undorec->xid;

		visi_info->xmin = undorec->xid;
		visi_info->cmin = undorec->cid;
		return result;
	}
	else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
		/* we don't care about tuple locks here. Follow the link to the
		 * previous UNDO record for this tuple. */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 undorec->type == ZSUNDO_TYPE_UPDATE)
	{
		if (undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			ZSUndoRec_Update *updaterec = (ZSUndoRec_Update *) undorec;
			if (next_tid)
				*next_tid = updaterec->newtid;
		}

		/*
		 * Deleted or updated-away. They are treated the same in an MVCC snapshot.
		 * They only need different treatment when updating or locking the row,
		 * in SatisfiesUpdate().
		 */
		if (xid_is_visible(snapshot, undorec->xid, undorec->cid, &aborted))
		{
			/* we can see the deletion */
			return false;
		}
		else
		{
			if (!aborted)
				*obsoleting_xid = undorec->xid;
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
		}
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
}

/*
 * Like HeapTupleSatisfiesSelf
 */
static bool
zs_SatisfiesSelf(ZSTidTreeScan *scan,
				 zstid *next_tid, ZSUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	ZSUndoRec  *undorec;
	ZSUndoRecPtr undo_ptr;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:
	if (undo_ptr.counter < scan->recent_oldest_undo.counter)
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto fetch_undo_record;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		visi_info->xmin = undorec->xid;
		visi_info->cmin = undorec->cid;

		/* Inserted tuple */
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
			return true;		/* inserted by me */
		else if (TransactionIdIsInProgress(undorec->xid))
			return false;
		else if (TransactionIdDidCommit(undorec->xid))
			return true;
		else
		{
			/* it must have aborted or crashed */
			return false;
		}
	}
	else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
		/* we don't care about tuple locks here. Follow the link to the
		 * previous UNDO record for this tuple. */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 undorec->type == ZSUNDO_TYPE_UPDATE)
	{
		if (undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			ZSUndoRec_Update *updaterec = (ZSUndoRec_Update *) undorec;
			if (next_tid)
				*next_tid = updaterec->newtid;
		}

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			/* deleted by me */
			return false;
		}

		if (TransactionIdIsInProgress(undorec->xid))
			return true;

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/*
			 * Deleter must have aborted or crashed. But we have to keep following the
			 * undo chain, to check if the insertion was visible in the first
			 * place.
			 */
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
		}

		return false;
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
}

/*
 * Like HeapTupleSatisfiesDirty
 */
static bool
zs_SatisfiesDirty(ZSTidTreeScan *scan,
				  zstid *next_tid, ZSUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = INVALID_SPECULATIVE_TOKEN;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:
	if (undo_ptr.counter < scan->recent_oldest_undo.counter)
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto fetch_undo_record;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		ZSUndoRec_Insert *insertrec = (ZSUndoRec_Insert *) undorec;
		snapshot->speculativeToken = insertrec->speculative_token;

		/*
		 * HACK: For SnapshotDirty need to set the values of xmin/xmax/... in
		 * snapshot based on tuples. Hence, can't set the visi_info values
		 * here similar to other snapshots. Only setting the value for
		 * TransactionIdIsInProgress().
		 */

		/* Inserted tuple */
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
			return true;		/* inserted by me */
		else if (TransactionIdIsInProgress(undorec->xid))
		{
			snapshot->xmin = undorec->xid;
			visi_info->xmin = undorec->xid;
			visi_info->cmin = undorec->cid;
			return true;
		}
		else if (TransactionIdDidCommit(undorec->xid))
		{
			return true;
		}
		else
		{
			/* it must have aborted or crashed */
			return false;
		}
	}
	else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
		/* locked tuple. */
		/* look at the previous UNDO record to find the insert record */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 undorec->type == ZSUNDO_TYPE_UPDATE)
	{
		if (undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			ZSUndoRec_Update *updaterec = (ZSUndoRec_Update *) undorec;
			if (next_tid)
				*next_tid = updaterec->newtid;
		}

		/* deleted or updated-away tuple */
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			/* deleted by me */
			return false;
		}

		if (TransactionIdIsInProgress(undorec->xid))
		{
			/*
			 * TODO: not required to set the snapshot's xmax here? As gets
			 * populated based on visi_info later in snapshot by caller.
			 */
			snapshot->xmax = undorec->xid;
			visi_info->xmax = undorec->xid;
			return true;
		}

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/*
			 * Deleter must have aborted or crashed. But we have to keep following the
			 * undo chain, to check if the insertion was visible in the first
			 * place.
			 */
			undo_ptr = undorec->prevundorec;
			goto fetch_undo_record;
		}

		return false;
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
}

/*
 * True if tuple might be visible to some transaction; false if it's
 * surely dead to everyone, ie, vacuumable.
 */
static bool
zs_SatisfiesNonVacuumable(ZSTidTreeScan *scan,
						  ZSUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;

	undo_ptr = visi_info->undoptr;

fetch_undo_record:

	/* Is it visible? */
	if (undo_ptr.counter < scan->recent_oldest_undo.counter)
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch_record(rel, undo_ptr);
	if (!undorec)
	{
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
		if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
		goto fetch_undo_record;
	}

	if (undorec->type == ZSUNDO_TYPE_INSERT)
	{
		visi_info->xmin = undorec->xid;
		visi_info->cmin = undorec->cid;

		/* Inserted tuple */
		if (TransactionIdIsInProgress(undorec->xid))
			return true;		/* inserter has not committed yet */

		if (TransactionIdDidCommit(undorec->xid))
			return true;

		/* it must have aborted or crashed */
		return false;
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 undorec->type == ZSUNDO_TYPE_UPDATE)
	{
		/* deleted or updated-away tuple */
		ZSUndoRecPtr	prevptr;

		if (TransactionIdIsInProgress(undorec->xid))
			return true;	/* delete-in-progress */
		else if (TransactionIdDidCommit(undorec->xid))
		{
			/*
			 * Deleter committed. But perhaps it was recent enough that some open
			 * transactions could still see the tuple.
			 */
			if (!GlobalVisTestIsRemovableXid(scan->snapshot->vistest, undorec->xid))
			{
				visi_info->nonvacuumable_status = ZSNV_RECENTLY_DEAD;
				return true;
			}

			return false;
		}

		/*
		 * The deleting transaction did not commit. But before concluding
		 * that the tuple is live, we have to check if the inserting
		 * XID is live.
		 */
		do {
			prevptr = undorec->prevundorec;

			if (prevptr.counter < scan->recent_oldest_undo.counter)
				return true;
			undorec = zsundo_fetch_record(rel, prevptr);
			if (!undorec)
			{
				scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel, false);
				if (undo_ptr.counter >= scan->recent_oldest_undo.counter)
					elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
						 undo_ptr.counter, undo_ptr.blkno, undo_ptr.offset);
				return true;
			}
		} while(undorec->type == ZSUNDO_TYPE_TUPLE_LOCK);

		Assert(undorec->type == ZSUNDO_TYPE_INSERT);

		if (TransactionIdIsInProgress(undorec->xid))
			return true;	/* insert-in-progress */
		else if (TransactionIdDidCommit(undorec->xid))
			return true;	/* inserted committed */

		/* inserter must have aborted or crashed */
		return false;
	}
	else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
	{
		/* look at the previous UNDO record, to find the Insert record */
		undo_ptr = undorec->prevundorec;
		goto fetch_undo_record;
	}
	else
		elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
}

/*
 * Like HeapTupleSatisfiesVisibility
 *
 * If next_tid is not NULL then gets populated for the tuple if tuple was
 * UPDATEd. *next_tid_p is set to the TID of the new row version.
 */
bool
zs_SatisfiesVisibility(ZSTidTreeScan *scan,
					   TransactionId *obsoleting_xid, zstid *next_tid,
					   ZSUndoSlotVisibility *visi_info)
{
	/* initialize as invalid, if we find valid one populate the same */
	if (next_tid)
		*next_tid = InvalidZSTid;

	/* The caller should've filled in the recent_oldest_undo pointer */
	Assert(scan->recent_oldest_undo.counter != 0);

	*obsoleting_xid = InvalidTransactionId;

	/*
	 * Items with invalid undo record are considered visible. Mostly META
	 * column stores the valid undo record, all other columns stores invalid
	 * undo pointer. Visibility check is performed based on META column and
	 * only if visible rest of columns are fetched. For in-place updates,
	 * columns other than META column may have valid undo record, in which
	 * case the visibility check needs to be performed for the same. META
	 * column can sometime also have items with invalid undo, see
	 * zsbt_undo_item_deletion().
	 */

	if (!IsZSUndoRecPtrValid(&visi_info->undoptr))
		return true;

	switch (scan->snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return zs_SatisfiesMVCC(scan, obsoleting_xid, next_tid, visi_info);

		case SNAPSHOT_SELF:
			return zs_SatisfiesSelf(scan, next_tid, visi_info);

		case SNAPSHOT_ANY:
			return zs_SatisfiesAny(scan, visi_info);

		case SNAPSHOT_TOAST:
			elog(ERROR, "SnapshotToast not implemented in zedstore");
			break;

		case SNAPSHOT_DIRTY:
			return zs_SatisfiesDirty(scan, next_tid, visi_info);

		case SNAPSHOT_HISTORIC_MVCC:
			elog(ERROR, "SnapshotHistoricMVCC not implemented in zedstore yet");
			break;

		case SNAPSHOT_NON_VACUUMABLE:
			return zs_SatisfiesNonVacuumable(scan, visi_info);
	}

	return false;				/* keep compiler quiet */
}
