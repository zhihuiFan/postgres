#include "postgres.h"

#include "access/cstore_api.h"
#include "access/zedstore_internal.h"


ZSTidTreeScan *
column_table_begin_scan(Relation relation, Snapshot snapshot)
{
	ZSTidTreeScan *tidscan = palloc(sizeof(ZSTidTreeScan));
	zsbt_tid_begin_scan(relation, MinZSTid, MaxPlusOneZSTid, snapshot, tidscan);
	tidscan->serializable = true;
	return tidscan;
}

/*
 * TODO: tupdesc is not necessary, a Form_pg_attribute is clearer.
 */
ZSAttrTreeScan *
column_table_column_begin_scan(Relation relation, TupleDesc tdesc, AttrNumber attno)
{
	ZSAttrTreeScan *attr_scan = palloc(sizeof(ZSAttrTreeScan));
	zsbt_attr_begin_scan(relation, tdesc, attno, attr_scan);
	return attr_scan;
}

zstid
column_table_next_row(ZSTidTreeScan *tid_tree, FetchDirection direction)
{
	return zsbt_tid_scan_next(tid_tree, direction);
}


void
column_table_fetch_column_value(ZSAttrTreeScan *btscan,
								zstid this_tid, Datum *datum,
								bool *isnull)
								/* /\* tdesc is .. *\/ */
								/* TupleDesc tdesc) */

{
	int natt = btscan->attno;
	Form_pg_attribute attr = btscan->attdesc;
	zsbt_attr_fetch(btscan, datum, isnull, this_tid);

	if (!(*isnull) && attr->attlen == -1 &&
		VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
	{
		MemoryContext oldcxt = CurrentMemoryContext;

		if (btscan->decoder.tmpcxt)
			MemoryContextSwitchTo(btscan->decoder.tmpcxt);
		*datum = zedstore_toast_flatten(btscan->rel, natt, this_tid, *datum);
		MemoryContextSwitchTo(oldcxt);
	}
	if (!isnull && attr->attlen == -1)
	{
		Assert (VARATT_IS_1B(datum) || INTALIGN(datum) == datum);
	}
}

void
column_table_column_end_scan(ZSAttrTreeScan *attr_scan)
{
	zsbt_attr_end_scan(attr_scan);
}

void column_table_end_scan(ZSTidTreeScan *tid_scan)
{
	zsbt_tid_end_scan(tid_scan);
}
