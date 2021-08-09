/*-------------------------------------------------------------------------
 *
 * uniquekey.c
 *	  Utilities for maintaining uniquekey.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekey.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"


/* Functions to populate UniqueKey */
static bool add_uniquekey_for_uniqueindex(PlannerInfo *root,
										  IndexOptInfo *unique_index,
										  List *mergeable_const_peer,
										  List *expr_opfamilies);

/* UniqueKey is subset of .. */
static bool uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey,
								  List *ecs, Relids relids);

/* Avoid useless UniqueKey. */
static bool unique_ecs_useful_for_distinct(PlannerInfo *root, List *ecs);
static bool unique_ecs_useful_for_merging(PlannerInfo *root, RelOptInfo *rel,
										  List *unique_ecs);
/* Helper functions to create UniqueKey. */
static UniqueKey *make_uniquekey(Bitmapset *unique_expr_indexes,
								 bool multi_null,
								 bool useful_for_distinct);
static void mark_rel_singlerow(PlannerInfo *root, RelOptInfo *rel);

/* Debug only */
static void print_uniquekey(PlannerInfo *root, RelOptInfo *rel);

/*
 * populate_baserel_uniquekeys
 */
void
populate_baserel_uniquekeys(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell	*lc;
	List	*mergeable_const_peer = NIL, *expr_opfamilies = NIL;

	/*
	 * ColX = {Const} AND ColY = {Const2} AND ColZ > {Const3},
	 * gather ColX and ColY into mergeable_const_peer.
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

		if (bms_is_empty(rinfo->left_relids))
			mergeable_const_peer = lappend(mergeable_const_peer, get_rightop(rinfo->clause));
		else if (bms_is_empty(rinfo->right_relids))
			mergeable_const_peer = lappend(mergeable_const_peer, get_leftop(rinfo->clause));
		else
			continue;
		expr_opfamilies = lappend(expr_opfamilies, rinfo->mergeopfamilies);
	}

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *)lfirst(lc);
		if (!index->unique || !index->immediate ||
			(index->indpred != NIL && !index->predOK))
			continue;

		if (add_uniquekey_for_uniqueindex(root, index,
										  mergeable_const_peer,
										  expr_opfamilies))
			/* Find a singlerow case, no need to go through any more. */
			return;
	}

	print_uniquekey(root, rel);
}

/*
 * relation_is_distinct_for
 *		Check if the relation is distinct for.
 */
bool
relation_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *distinct_pathkey)
{
	ListCell	*lc;
	List	*lecs = NIL;
	Relids	relids = NULL;
	foreach(lc, distinct_pathkey)
	{
		PathKey *pathkey = lfirst(lc);
		lecs = lappend(lecs, pathkey->pk_eclass);
		/*
		 * Note that ec_relids doesn't include child member, but
		 * distinct would not operate on childrel as well.
		 */
		relids = bms_union(relids, pathkey->pk_eclass->ec_relids);
	}

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst(lc);
		if (ukey->multi_nulls)
			continue;

		if (uniquekey_contains_in(root, ukey, lecs, relids))
			return true;
	}
	return false;
}

/*
 * add_uniquekey_for_uniqueindex
 *	 populate a UniqueKey if necessary, return true iff the UniqueKey is an
 * SingleRow.
 */
static bool
add_uniquekey_for_uniqueindex(PlannerInfo *root, IndexOptInfo *unique_index,
							  List *mergeable_const_peer, List *expr_opfamilies)
{
	List	*unique_exprs = NIL, *unique_ecs = NIL;
	ListCell	*indexpr_item;
	int	c = 0;
	RelOptInfo *rel = unique_index->rel;
	bool	multinull = false;
	bool	used_for_distinct = false;
	Bitmapset *unique_exprs_index;

	indexpr_item = list_head(unique_index->indexprs);
	/* Gather all the non-const exprs */
	for (c = 0; c < unique_index->nkeycolumns; c++)
	{
		int attr = unique_index->indexkeys[c];
		Expr *expr;
		bool	matched_const = false;
		ListCell	*lc1, *lc2;
		if (attr > 0)
		{
			Var *var;
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
			var = castNode(Var, expr);
			Assert(IsA(expr, Var));
			if (!bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
							  rel->notnull_attrs[0]))
				multinull = true;
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
			/* We can't grantee an FuncExpr will not return NULLs */
			multinull = true;
		}
		else /* attr < 0 */
		{
			/* Index on OID is possible, not handle it for now. */
			return false;
		}

		/*
		 * Check index_col = Const case with regarding to opfamily checking
		 * If so, we can remove the index_col from the final UniqueKey->exprs.
		 */
		forboth(lc1, mergeable_const_peer, lc2, expr_opfamilies)
		{
			if (list_member_oid((List *) lfirst(lc2), unique_index->opfamily[c]) &&
				match_index_to_operand((Node *) lfirst(lc1), c, unique_index))
			{
				matched_const = true;
				break;
			}
		}

		if (matched_const)
			continue;

		unique_exprs = lappend(unique_exprs, expr);
	}

	if (unique_exprs == NIL)
	{
		/*
		 * SingleRow case. Checking if it is useful is ineffective
		 * so just keep it.
		 */
		mark_rel_singlerow(root, rel);
		return true;
	}

	unique_ecs = build_equivalanceclass_list_for_exprs(root, unique_exprs, rel);

	if (unique_ecs == NIL)
	{
		/* It is neither used in distinct_pathkey nor mergeable clause */
		return false;
	}

	/*
	 * Check if we need to setup the UniqueKey and set the used_for_distinct accordingly.
	 */
	if (unique_ecs_useful_for_distinct(root, unique_ecs))
	{
		used_for_distinct = true;
	}
	else if (!unique_ecs_useful_for_merging(root, rel, unique_ecs))
		/*
		 * Neither used in distinct pathkey nor used in mergeable clause.
		 * this is possible even if unique_ecs != NIL.
		 */
		return false;
	else
	{
		/*
		 * unique_ecs_useful_for_merging(root, rel, unique_ecs) is true,
		 * we did nothing in this case.
		 */
	}
	unique_exprs_index = bms_make_singleton(list_length(root->unique_exprs));
	root->unique_exprs = lappend(root->unique_exprs, unique_ecs);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  make_uniquekey(unique_exprs_index,
											 multinull,
											 used_for_distinct));
	return false;
}
/*
 * uniquekey_contains_in
 *	Return if UniqueKey contains in the list of EquivalenceClass
 * or the UniqueKey's SingleRow contains in relids.
 *
 */
static bool
uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey, List *ecs, Relids relids)
{
	int i = -1;
	while ((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
	{
		Node *exprs = list_nth(root->unique_exprs, i);
		if (IsA(exprs, SingleRow))
		{
			SingleRow *singlerow = castNode(SingleRow, exprs);
			if (!bms_is_member(singlerow->relid, relids))
				/*
				 * UniqueKey request a ANY expr on relid on the relid(which
				 * indicates we don't have other EquivalenceClass for this
				 * relation), but the relid doesn't contains in relids, which
				 * indicate there is no such Expr in target, then we are sure
				 * to return false.
				 */
				return false;
			else
			{
				/*
				 * We have SingleRow on relid, and the relid is in relids.
				 * We don't need to check any more for this expr. This is
				 * right for sure.
				 */
			}
		}
		else
		{
			Assert(IsA(exprs, List));
			if (!list_is_subset_ptr((List *)exprs, ecs))
				return false;
		}
	}
	return true;
}

/*
 * unique_ecs_useful_for_distinct
 *	return true if all the EquivalenceClass in ecs exists in root->distinct_pathkey.
 */
static bool
unique_ecs_useful_for_distinct(PlannerInfo *root, List *ecs)
{
	ListCell *lc;
	foreach(lc, ecs)
	{
		EquivalenceClass *ec = lfirst_node(EquivalenceClass, lc);
		ListCell *p;
		bool found = false;
		foreach(p,  root->distinct_pathkeys)
		{
			PathKey *pathkey = lfirst_node(PathKey, p);
			/*
			 * Both of them should point to an element in root->eq_classes.
			 * so the address should be same. and equal function doesn't
			 * support EquivalenceClass yet.
			 */
			if (ec == pathkey->pk_eclass)
			{
				found = true;
				break;
			}
		}
		if (!found)
			return false;
	}
	return true;
}

/*
 * unique_ecs_useful_for_merging
 *	return true if all the unique_ecs exists in rel's join restrictInfo.
 */
static bool
unique_ecs_useful_for_merging(PlannerInfo *root, RelOptInfo *rel, List *unique_ecs)
{
	ListCell	*lc;

	foreach(lc, unique_ecs)
	{
		EquivalenceClass *ec = lfirst(lc);
		if (!ec_useful_for_merging(root, rel, ec))
			return false;
	}

	return true;
}
/*
 *	make_uniquekey
 */
static UniqueKey *
make_uniquekey(Bitmapset *unique_expr_indexes, bool multi_null, bool useful_for_distinct)
{
	UniqueKey *ukey = makeNode(UniqueKey);
	ukey->unique_expr_indexes = unique_expr_indexes;
	ukey->multi_nulls = multi_null;
	ukey->use_for_distinct = useful_for_distinct;
	return ukey;
}

/*
 * mark_rel_singlerow
 *	mark a relation as singlerow.
 */
static void
mark_rel_singlerow(PlannerInfo *root, RelOptInfo *rel)
{
	int exprs_pos = list_length(root->unique_exprs);
	Bitmapset *unique_exprs_index = bms_make_singleton(exprs_pos);
	SingleRow *singlerow = makeNode(SingleRow);
	singlerow->relid = rel->relid;
	root->unique_exprs = lappend(root->unique_exprs, singlerow);
	rel->uniquekeys = list_make1(make_uniquekey(unique_exprs_index,
												false /* multi-null */,
												true /* arbitrary decision */));
}

/*
 * print_uniquekey
 *	Used for easier reivew, should be removed before commit.
 */
static void
print_uniquekey(PlannerInfo *root, RelOptInfo *rel)
{
	if (false)
	{
		ListCell	*lc;
		elog(INFO, "Rel = %s", bmsToString(rel->relids));
		foreach(lc, rel->uniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			int i = -1;
			elog(INFO, "UNIQUEKEY{indexes=%s, multinull=%d}",
				 bmsToString(ukey->unique_expr_indexes),
				 ukey->multi_nulls
				);

			while ((i = bms_next_member(ukey->unique_expr_indexes, i)) >= 0)
			{
				Node *node = (Node *) list_nth(root->unique_exprs, i);
				if (IsA(node, SingleRow))
					elog(INFO,
						 "Expr(%d) SingleRow{relid = %d}",
						 i, castNode(SingleRow, node)->relid);
				else
					elog(INFO,
						 "EC(%d), %s", i, nodeToString(node)
						);
			}
		}
	}
}
