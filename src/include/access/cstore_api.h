#include "postgres.h"

#include "access/zedstore_internal.h"


extern ZSTidTreeScan *column_table_begin_scan(Relation relation, Snapshot snapshot);
extern ZSAttrTreeScan *column_table_column_begin_scan(Relation relation, TupleDesc tdesc, AttrNumber attno);

extern zstid column_table_next_row(ZSTidTreeScan *tid_tree, FetchDirection direction);

extern void column_table_fetch_column_value(ZSAttrTreeScan *treeScan, zstid this_tid,
											Datum *datum, bool *isnull);
extern void column_table_column_end_scan(ZSAttrTreeScan *);
extern void column_table_end_scan(ZSTidTreeScan *);
