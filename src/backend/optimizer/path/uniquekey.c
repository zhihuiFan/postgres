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
										  List *truncatable_exprs,
										  List *expr_opfamilies);
static bool unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ec_indexes);

/* Helper functions to create UniqueKey. */
static UniqueKey * make_uniquekey(Bitmapset *eclass_indexes,
								  bool useful_for_distinct);
static void mark_rel_singlerow(RelOptInfo *rel, int relid);

static UniqueKey * rel_singlerow_uniquekey(RelOptInfo *rel);
static bool uniquekey_contains_multinulls(PlannerInfo *root, RelOptInfo *rel, UniqueKey * ukey);

/* Debug only */
static void print_uniquekey(PlannerInfo *root, RelOptInfo *rel);


/*
 * populate_baserel_uniquekeys
 *
 *		UniqueKey on baserel comes from unique indexes. Any expression
 * which equals with Const can be stripped and the left expression are
 * still unique.
 */
void
populate_baserel_uniquekeys(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *lc;
	List	   *truncatable_exprs = NIL,
			   *expr_opfamilies = NIL;

	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->mergeopfamilies == NIL)
			continue;

		if (!IsA(rinfo->clause, OpExpr))
			continue;

		if (bms_is_empty(rinfo->left_relids))
			truncatable_exprs = lappend(truncatable_exprs, get_rightop(rinfo->clause));
		else if (bms_is_empty(rinfo->right_relids))
			truncatable_exprs = lappend(truncatable_exprs, get_leftop(rinfo->clause));
		else
			continue;

		expr_opfamilies = lappend(expr_opfamilies, rinfo->mergeopfamilies);
	}

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

		if (!index->unique || !index->immediate ||
			(index->indpred != NIL && !index->predOK))
			continue;

		if (add_uniquekey_for_uniqueindex(root, index,
										  truncatable_exprs,
										  expr_opfamilies))
			/* Find a singlerow case, no need to go through other indexes. */
			return;
	}

	print_uniquekey(root, rel);
}


/*
 * add_uniquekey_for_uniqueindex
 *
 *		 populate a UniqueKey if it is interesting, return true iff the
 * UniqueKey is an SingleRow. Only the interesting UniqueKeys are kept.
 */
static bool
add_uniquekey_for_uniqueindex(PlannerInfo *root, IndexOptInfo *unique_index,
							  List *truncatable_exprs, List *expr_opfamilies)
{
	List	   *unique_exprs = NIL;
	Bitmapset  *unique_ecs = NULL;
	ListCell   *indexpr_item;
	RelOptInfo *rel = unique_index->rel;
	bool		used_for_distinct;
	int			c;

	indexpr_item = list_head(unique_index->indexprs);

	for (c = 0; c < unique_index->nkeycolumns; c++)
	{
		int			attr = unique_index->indexkeys[c];
		Expr	   *expr;		/* The candidate for UniqueKey expression. */
		bool		matched_const = false;
		ListCell   *lc1,
				   *lc2;

		if (attr > 0)
		{
			Var		   *var;

			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
			var = castNode(Var, expr);
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
		}
		else					/* attr < 0 */
		{
			/* Index on OID is possible, not handle it for now. */
			return false;
		}

		/* Ignore the expr which are equals to const. */
		forboth(lc1, truncatable_exprs, lc2, expr_opfamilies)
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
		/* single row is always interesting. */
		mark_rel_singlerow(rel, rel->relid);
		return true;
	}

	/*
	 * if no EquivalenceClass is found for any exprs in unique exprs, we are
	 * sure the whole exprs are not in the DISTINCT clause or mergeable join
	 * clauses. so it is not interesting.
	 */
	unique_ecs = build_ec_positions_for_exprs(root, unique_exprs, rel);
	if (unique_ecs == NULL)
		return false;

	used_for_distinct = unique_ecs_useful_for_distinct(root, unique_ecs);


	rel->uniquekeys = lappend(rel->uniquekeys,
							  make_uniquekey(unique_ecs,
											 used_for_distinct));
	return false;
}

/*
 *	make_uniquekey
 *		Based on UnqiueKey rules, it is impossible for a UnqiueKey
 * which have eclass_indexes and relid both set. This function just
 * handle eclass_indexes case.
 */
static UniqueKey *
make_uniquekey(Bitmapset *eclass_indexes, bool useful_for_distinct)
{
	UniqueKey  *ukey = makeNode(UniqueKey);

	ukey->eclass_indexes = eclass_indexes;
	ukey->relid = 0;
	ukey->use_for_distinct = useful_for_distinct;
	return ukey;
}

/*
 * mark_rel_singlerow
 *	mark a relation as singlerow.
 */
static void
mark_rel_singlerow(RelOptInfo *rel, int relid)
{
	UniqueKey  *ukey = makeNode(UniqueKey);

	ukey->relid = relid;
	rel->uniquekeys = list_make1(ukey);
}

/*
 *
 *	Return the UniqueKey if rel is a singlerow Relation. othwise
 * return NULL.
 */
static UniqueKey *
rel_singlerow_uniquekey(RelOptInfo *rel)
{
	if (rel->uniquekeys != NIL)
	{
		UniqueKey  *ukey = linitial_node(UniqueKey, rel->uniquekeys);

		if (ukey->relid)
			return ukey;
	}
	return NULL;
}

/*
 * print_uniquekey
 *	Used for easier reivew, should be removed before commit.
 */
static void
print_uniquekey(PlannerInfo *root, RelOptInfo *rel)
{
	if (!enable_geqo)
	{
		ListCell   *lc;

		elog(INFO, "Rel = %s", bmsToString(rel->relids));
		foreach(lc, rel->uniquekeys)
		{
			UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

			elog(INFO, "UNIQUEKEY{indexes=%s, singlerow_rels=%d, use_for_distinct=%d}",
				 bmsToString(ukey->eclass_indexes),
				 ukey->relid,
				 ukey->use_for_distinct);
		}
	}
}

/*
 *	is it possible that the var contains multi NULL values in the given
 * RelOptInfo rel?
 */
static bool
var_is_nullable(PlannerInfo *root, Var *var, RelOptInfo *rel)
{
	RelOptInfo *base_rel;

	/* check if the outer join can add the NULL values.  */
	if (bms_overlap(var->varnullingrels, rel->relids))
		return true;

	/* check if the user data has the NULL values. */
	base_rel = root->simple_rel_array[var->varno];
	return !bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber, base_rel->notnullattrs);
}


/*
 * uniquekey_contains_multinulls
 *
 *	Check if the uniquekey contains nulls values.
 */
static bool
uniquekey_contains_multinulls(PlannerInfo *root, RelOptInfo *rel, UniqueKey * ukey)
{
	int			i = -1;

	while ((i = bms_next_member(rel->eclass_indexes, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth_node(EquivalenceClass, root->eq_classes, i);
		ListCell   *lc;

		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = lfirst_node(EquivalenceMember, lc);
			Var		   *var;

			var = (Var *) em->em_expr;

			if (!IsA(var, Var))
				continue;

			if (var_is_nullable(root, var, rel))
				return true;
			else

				/*
				 * If any one of member in the EC is not nullable, we all the
				 * members are not nullable since they are equal with each
				 * other.
				 */
				break;
		}
	}

	return false;
}


/*
 * relation_is_distinct_for
 *
 * Check if the rel is distinct for distinct_pathkey.
 */
bool
relation_is_distinct_for(PlannerInfo *root, RelOptInfo *rel, List *distinct_pathkey)
{
	ListCell   *lc;
	UniqueKey  *singlerow_ukey = rel_singlerow_uniquekey(rel);
	Bitmapset  *pathkey_bm = NULL;

	if (singlerow_ukey)
	{
		return !uniquekey_contains_multinulls(root, rel, singlerow_ukey);
	}

	foreach(lc, distinct_pathkey)
	{
		PathKey    *pathkey = lfirst_node(PathKey, lc);
		int			pos = list_member_ptr_pos(root->eq_classes, pathkey->pk_eclass);

		if (pos == -1)
			return false;

		pathkey_bm = bms_add_member(pathkey_bm, pos);
	}

	foreach(lc, rel->uniquekeys)
	{
		UniqueKey  *ukey = lfirst_node(UniqueKey, lc);

		if (bms_is_subset(ukey->eclass_indexes, pathkey_bm) &&
			!uniquekey_contains_multinulls(root, rel, ukey))
			return true;
	}

	return false;
}

/*
 * unique_ecs_useful_for_distinct
 *
 *	Return true if all the EquivalenceClass for ecs exists in root->distinct_pathkey.
 */
static bool
unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ec_indexes)
{
	int			i = -1;

	while ((i = bms_next_member(ec_indexes, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth(root->eq_classes, i);
		ListCell   *p;
		bool		found = false;

		foreach(p, root->distinct_pathkeys)
		{
			PathKey    *pathkey = lfirst_node(PathKey, p);

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
