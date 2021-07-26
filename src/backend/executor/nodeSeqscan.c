/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *node);
bool enable_column_scan = false;

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		if (table_scans_leverage_column_projection(node->ss.ss_currentRelation))
		{
			Scan *planNode = (Scan *)node->ss.ps.plan;
			int rti = planNode->scanrelid;
			RangeTblEntry *rte = list_nth(estate->es_plannedstmt->rtable, rti - 1);
			scandesc = table_beginscan_with_column_projection(node->ss.ss_currentRelation,
															  estate->es_snapshot,
															  0, NULL,
															  rte->scanCols);
			if (enable_column_scan)
			{
				TupleDesc tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
				int i;
				zsbt_tid_begin_scan(node->ss.ss_currentRelation, MinZSTid,
									MaxPlusOneZSTid,
									estate->es_snapshot, &node->ss.tid_scan);
			    node->ss.tid_scan.serializable = true;
				node->ss.attr_scans = palloc0(
					sizeof(ZSAttrTreeScan *) * (tupdesc->natts));
				for(i = 0; i < tupdesc->natts; i++)
				{
					zsbt_attr_begin_scan(node->ss.ss_currentRelation, tupdesc, i+1, &node->ss.attr_scans[i]);
				}
			}


		}
		else
		{
			scandesc = table_beginscan(node->ss.ss_currentRelation,
									   estate->es_snapshot,
									   0, NULL);
		}
		node->ss.ss_currentScanDesc = scandesc;
	}

	if (node->ss.attr_scans != NULL)
	{
		/* Test APIs */
		zstid curtid =  zsbt_tid_scan_next(&node->ss.tid_scan, direction);
		TupleDesc tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
		int i;
		// elog(INFO, "Fetch data for zstid %lu", curtid);
		if (curtid != 0)
		{
			for(i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);
				if (att->atttypid == INT4OID)
				{
					Datum datum;
					bool isnull;
					Form_pg_attribute attr = node->ss.attr_scans[i].attdesc;
					ZSAttrTreeScan *btscan = &node->ss.attr_scans[i];

					if (!zsbt_attr_fetch(&node->ss.attr_scans[i], &datum, &isnull, curtid))
						zsbt_fill_missing_attribute_value(tupdesc, node->ss.attr_scans[i].attno, &datum, &isnull);
					if (!isnull && attr->attlen == -1 &&
						VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
					{
						MemoryContext oldcxt = CurrentMemoryContext;

						if (btscan->decoder.tmpcxt)
							MemoryContextSwitchTo(btscan->decoder.tmpcxt);
						datum = zedstore_toast_flatten(node->ss.ss_currentRelation, attr->attnum, curtid, datum);
						MemoryContextSwitchTo(oldcxt);
					}

					/* Check that the values coming out of the b-tree are aligned properly */
					if (!isnull && attr->attlen == -1)
					{
						Assert (VARATT_IS_1B(datum) || INTALIGN(datum) == datum);
					}

					//	elog(INFO, "idx: %d col name: %s, colno: %d value = %d", i,
					// att->attname.data, att->attnum, DatumGetInt(datum));
				}
			}
		}
	}
	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecSeqScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (node->ss.attr_scans != NULL)
	{
		TupleDesc tupdesc = node->ss.ss_currentRelation->rd_att;
		int i;
		zsbt_tid_end_scan(&node->ss.tid_scan);
		for(i = 0; i < tupdesc->natts; i++)
			zsbt_attr_end_scan(&node->ss.attr_scans[i]);
	}
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;
	Bitmapset *proj = NULL;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);

	if (table_scans_leverage_column_projection(node->ss.ss_currentRelation))
	{
		proj = PopulateNeededColumnsForScan(&node->ss,
											node->ss.ss_currentRelation->rd_att->natts);
	}

	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan, proj);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;
	Bitmapset *proj = NULL;

	/*
	 * FIXME: this is duplicate work with ExecSeqScanInitializeDSM. In future
	 * plan will have the we have projection list, then this overhead will not exist.
	 */
	if (table_scans_leverage_column_projection(node->ss.ss_currentRelation))
	{
		proj = PopulateNeededColumnsForScan(&node->ss,
											node->ss.ss_currentRelation->rd_att->natts);
	}

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan, proj);
}
