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

static bool populate_joinrel_uniquekey_for_rel(PlannerInfo *root, RelOptInfo *joinrel,
											   RelOptInfo *rel, RelOptInfo *other_rel,
											   List *restrictlist);
static void populate_joinrel_composite_uniquekey(PlannerInfo *root,
												 RelOptInfo *joinrel,
												 RelOptInfo *outerrel,
												 RelOptInfo *innerrel);
/* UniqueKey is subset of .. */
static bool uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey,
								  List *ecs, RelOptInfo *joinrel);

/* Avoid useless UniqueKey. */
static bool unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ecs);
static bool is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey *ukey,
										  RelOptInfo *joinrel);

/* Helper functions to create UniqueKey. */
static UniqueKey *make_uniquekey(Bitmapset *eclass_indexes,
								 bool useful_for_distinct);
static void mark_rel_singlerow(RelOptInfo *rel, int relid);

/* Debug only */
static void print_uniquekey(PlannerInfo *root, RelOptInfo *rel);


/*
 * populate_baserel_uniquekeys
 *
 *	UniqueKey on baserel comes from unique indexes. Any expression
 * in unique indexes which equals to Const for this rel should be
 * truncated and the left expressiones are still unique.
 */
void
populate_baserel_uniquekeys(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell	*lc;
	List	*truncatable_exprs = NIL, *expr_opfamilies = NIL;

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
		IndexOptInfo *index = (IndexOptInfo *)lfirst(lc);
		if (!index->unique || !index->immediate ||
			(index->indpred != NIL && !index->predOK))
			continue;

		if (add_uniquekey_for_uniqueindex(root, index,
										  truncatable_exprs,
										  expr_opfamilies))
			/* Find a singlerow case, no need to go through any more. */
			return;
	}

	print_uniquekey(root, rel);
}

/*
 * populate_joinrel_uniquekeys
 *
 */
void
populate_joinrel_uniquekeys(PlannerInfo *root, RelOptInfo *joinrel,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *restrictlist, JoinType jointype)
{
	bool outeruk_still_valid, inneruk_still_valid;

	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		joinrel->uniquekeys = outerrel->uniquekeys;
		return;
	}

	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return;

	outeruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, outerrel,
															 innerrel, restrictlist);
	inneruk_still_valid = populate_joinrel_uniquekey_for_rel(root, joinrel, innerrel,
															 outerrel, restrictlist);


	if (!outeruk_still_valid || !inneruk_still_valid)
		populate_joinrel_composite_uniquekey(root, joinrel,
											 outerrel,
											 innerrel);

	print_uniquekey(root, joinrel);
}

/*
 * add_uniquekey_for_uniqueindex
 *
 *	 populate a UniqueKey if necessary, return true iff the UniqueKey is an
 * SingleRow. We only keep the interesting UniqueKeys. Per our current
 * UniqueKey user case, any expression in interesting Unqiuekeys has a
 * related EquivalenceClass for sure.
 */
static bool
add_uniquekey_for_uniqueindex(PlannerInfo *root, IndexOptInfo *unique_index,
							  List *truncatable_exprs, List *expr_opfamilies)
{
	List	*unique_exprs = NIL;
	Bitmapset *unique_ecs = NULL;
	ListCell	*indexpr_item;
	RelOptInfo *rel = unique_index->rel;
	bool	used_for_distinct = false;
	int c;

	indexpr_item = list_head(unique_index->indexprs);

	for (c = 0; c < unique_index->nkeycolumns; c++)
	{
		int attr = unique_index->indexkeys[c];
		Expr *expr;  /* The candidate for UniqueKey expression. */
		bool	matched_const = false;
		ListCell	*lc1, *lc2;

		if (attr > 0)
		{
			Var *var;
			expr = list_nth_node(TargetEntry, unique_index->indextlist, c)->expr;
			var = castNode(Var, expr);
		}
		else if (attr == 0)
		{
			/* Expression index */
			expr = lfirst(indexpr_item);
			indexpr_item = lnext(unique_index->indexprs, indexpr_item);
		}
		else /* attr < 0 */
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
		/* All the exprs equals to Const. */
		mark_rel_singlerow(rel, rel->relid);
		return true;
	}

	/*
	 * Find the related EquivalenceClass for the exprs.
	 */
	unique_ecs = build_equivalanceclass_list_for_exprs(root, unique_exprs, rel);

	if (unique_ecs == NULL)
	{
		/* Not an interesting case. */
		return false;
	}

	/*
	 * Check if we need to setup the UniqueKey and set the used_for_distinct accordingly.
	 */
	if (unique_ecs_useful_for_distinct(root, unique_ecs))
	{
		used_for_distinct = true;
	}
	/* else if (!unique_ecs_useful_for_merging(root, rel, unique_ecs)) */
	/* 	/\* */
	/* 	 * Neither used in distinct pathkey nor used in mergeable clause. */
	/* 	 * this is possible even if unique_ecs != NIL. */
	/* 	 *\/ */
	/* 	return false; */
	else
	{
		/*
		 * unique_ecs_useful_for_merging(root, rel, unique_ecs) is true,
		 * we did nothing in this case.
		 */
	}

	rel->uniquekeys = lappend(rel->uniquekeys,
							  make_uniquekey(unique_ecs,
											 used_for_distinct));
	return false;
}

/*
 * populate_joinrel_uniquekey_for_rel
 *
 *    Check if we have pattern rel.any_columns = other_rel.unique_key_columns.
 * if so, the UniqueKey on rel side is still valid so we add the UniqueKey 
 * from rel to joinrel, return true to indicate this happens.
 */
static bool
populate_joinrel_uniquekey_for_rel(PlannerInfo *root, RelOptInfo *joinrel,
								   RelOptInfo *rel, RelOptInfo *other_rel,
								   List *restrictlist)
{
	bool	rel_keep_unique = false;
	List *other_ecs = NIL;
	Relids	other_relids = NULL;
	ListCell	*lc;

	/*
	 * Gather all the other ECs regarding to rel, if all the unique ecs contains
	 * in this list, then it hits our expectations.
	 */
	foreach(lc, restrictlist)
	{
		RestrictInfo *r = lfirst_node(RestrictInfo, lc);

		if (r->mergeopfamilies == NIL)
			continue;

		Assert(r->right_ec != NULL && r->left_ec != NULL);

		if (!IsA(r->clause, OpExpr))
			continue;

		if (bms_equal(r->left_relids, rel->relids) && r->right_ec != NULL)
		{
			other_ecs = lappend(other_ecs, r->right_ec);
			other_relids = bms_add_members(other_relids, r->right_relids);
		}
		else if (bms_equal(r->right_relids, rel->relids) && r->left_ec != NULL)
		{
			other_ecs = lappend(other_ecs, r->right_ec);
			other_relids = bms_add_members(other_relids, r->left_relids);
		}
	}

	foreach(lc, other_rel->uniquekeys)
	{
		UniqueKey *other_ukey = lfirst_node(UniqueKey, lc);
		if (uniquekey_contains_in(root, other_ukey, other_ecs, joinrel))
		{
			rel_keep_unique = true;
			break;
		}
	}

	if (!rel_keep_unique)
		return false;

	foreach(lc, rel->uniquekeys)
	{

		UniqueKey *ukey = lfirst_node(UniqueKey, lc);

		if (is_uniquekey_useful_afterjoin(root, ukey, joinrel))
		{
			joinrel->uniquekeys = lappend(joinrel->uniquekeys, ukey);
		}
		else
		{
			/*
			 * XXX: if all of them are not useful, we still need to return
			 * true to indicates composited uniquekey is not needed since
			 * they are not useful for sure.
			 */
		}


	}

	return true;
}


/*
 * Populate_joinrel_composited_uniquekey
 *
 *	A composited unqiuekey is valid no matter with join type and restrictlist.
 *
 * XXX: With the new Unique Key data struct, this function can be implemented
 * more simple. Just some bms_union is OK.
 */
static void
populate_joinrel_composite_uniquekey(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel)
{
	ListCell *lc;

	foreach(lc, outerrel->uniquekeys)
	{
		UniqueKey *outer_ukey = lfirst_node(UniqueKey, lc);
		ListCell	*lc2;

		/*
		 * XXX: Cached the result somehow before?
		 */
		if (!is_uniquekey_useful_afterjoin(root, outer_ukey, joinrel))
			continue;

		foreach(lc2, innerrel->uniquekeys)
		{
			UniqueKey	*inner_ukey = lfirst_node(UniqueKey, lc2);
			UniqueKey	*comp_ukey;

			if (!is_uniquekey_useful_afterjoin(root, inner_ukey, joinrel))
				continue;

			comp_ukey = make_uniquekey(
				bms_union(outer_ukey->eclass_indexes, inner_ukey->eclass_indexes),
				outer_ukey->use_for_distinct || inner_ukey->use_for_distinct);
			joinrel->uniquekeys = lappend(joinrel->uniquekeys, comp_ukey);
		}
	}
}

/*
 * uniquekey_contains_in
 *
 *	  Check if any eclass in ukey appears in EquivalenceClass List.
 */
static bool
uniquekey_contains_in(PlannerInfo *root, UniqueKey *ukey, List *ecs, RelOptInfo *joinrel)
{
	int i = -1;

	if (ukey->relid != 0)
	{
		/* UniqueKey rule 2.1.  */
		return true;
	}
	
	/*
	 * The eclass required by ukey must appear in eclass list which extracts from
	 * restrictinfo_list.
	 */
	while ((i = bms_next_member(ukey->eclass_indexes, i)) >= 0)
	{
		EquivalenceClass *eclass = list_nth_node(EquivalenceClass,
												 root->eq_classes,
												 i);
		if (!list_member_ptr(ecs, eclass))
			return false;
	}
	return true;
}

/*
 * unique_ecs_useful_for_distinct
 *
 *	Return true if all the EquivalenceClass for ecs exists in root->distinct_pathkey.
 */
static bool
unique_ecs_useful_for_distinct(PlannerInfo *root, Bitmapset *ecs)
{
	int i = -1;

	while ((i = bms_next_member(ecs, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth(root->eq_classes, i);
		ListCell *p;
		bool found = false;
		foreach(p, root->distinct_pathkeys)
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
 * is_uniquekey_useful_afterjoin
 *
 *  is useful when it contains in distinct_pathkey or in mergable join clauses.
 */
static bool
is_uniquekey_useful_afterjoin(PlannerInfo *root, UniqueKey *ukey,
							  RelOptInfo *joinrel)
{
	int	i = -1;

	if (ukey->use_for_distinct)
		return true;

	while((i = bms_next_member(ukey->eclass_indexes, i)) >= 0)
	{
		EquivalenceClass *eclass = list_nth_node(EquivalenceClass, root->eq_classes, i);
		if (!eclass_useful_for_joining(root, joinrel, eclass))
			return false;
	}
	return true;
}


/*
 *	make_uniquekey
 * 		Based on UnqiueKey rules, it is impossible for a UnqiueKey
 * which have eclass_indexes and relid both set. This function just
 * handle eclass_indexes case.
 */
static UniqueKey *
make_uniquekey(Bitmapset *eclass_indexes,  bool useful_for_distinct)
{
	UniqueKey *ukey = makeNode(UniqueKey);
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
	UniqueKey *ukey = makeNode(UniqueKey);
	ukey->relid = relid;
	rel->uniquekeys = list_make1(ukey);
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
		ListCell	*lc;
		elog(INFO, "Rel = %s", bmsToString(rel->relids));
		foreach(lc, rel->uniquekeys)
		{
			UniqueKey *ukey = lfirst_node(UniqueKey, lc);
			elog(INFO, "UNIQUEKEY{indexes=%s, singlerow_rels=%d, use_for_distinct=%d}",
				 bmsToString(ukey->eclass_indexes),
				 ukey->relid,
				 ukey->use_for_distinct);
		}
	}
}
