/*-------------------------------------------------------------------------
 *
 * cdbgroupingpaths.c
 *	  Routines to aid in planning grouping queries for parallel
 *    execution.  This is, essentially, an extension of the file
 *    optimizer/prep/planner.c, although some functions are not
 *    externalized.
 *
 * Portions Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroupingpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbgroupingpaths.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbpath.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "executor/execHHashagg.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"

typedef enum {
	INVALID = 0,
	SINGLEDQA,
	MULTIDQAS,
	MIXEDDQAS
} DQATYPE ;

/*
 * For convenience, we collect various inputs and intermediate planning results
 * in this struct, instead of passing a dozen arguments to all subroutines.
 */
typedef struct
{
	/* Inputs from the caller */
	PathTarget *target;
	PathTarget *partial_grouping_target;
	double		dNumGroups;
	const AggClauseCosts *agg_costs;
	const AggClauseCosts *agg_partial_costs;
	const AggClauseCosts *agg_final_costs;
} cdb_agg_planning_context;

typedef struct
{
	PathTarget *input_target;
	List       *dqa_group_clause;
	Bitmapset  *dqas_ref_bm;
	int         dqas_num;
	int        maxref;
} cdb_distinct_info;

static void add_twostage_group_agg_path(PlannerInfo *root,
										Path *path,
										bool is_sorted,
										cdb_agg_planning_context *ctx,
										RelOptInfo *output_rel);

static void add_twostage_hash_agg_path(PlannerInfo *root,
									   Path *path,
									   cdb_agg_planning_context *ctx,
									   RelOptInfo *output_rel);

static void add_single_dqa_hash_agg_path(PlannerInfo *root,
										 Path *path,
										 cdb_agg_planning_context *ctx,
										 RelOptInfo *output_rel,
										 PathTarget *input_target,
										 List	   *dqa_group_clause);

static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
                            Path *path,
                            cdb_agg_planning_context *ctx,
                            RelOptInfo *output_rel,
                            cdb_distinct_info *info);

static DQATYPE analyze_dqas(PlannerInfo *root,
                            Path *path,
                            cdb_agg_planning_context *ctx,
							cdb_distinct_info *info);
/*
                            PathTarget **input_target_ret_p,
                            List       **group_clause_ret_p,
                            int        **distref_p,
                            int        *maxref);
                            */

static PathTarget *
strip_aggdistinct(PathTarget *target);

/*
 * Function: cdb_grouping_planner
 *
 * This is basically an extension of the function create_grouping_paths() from
 * planner.c.  It creates two- and three-stage Paths to implement aggregates
 * and/or GROUP BY.
 */
void
cdb_create_twostage_grouping_paths(PlannerInfo *root,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   PathTarget *target,
								   PathTarget *partial_grouping_target,
								   bool can_sort,
								   bool can_hash,
								   double dNumGroups,
								   const AggClauseCosts *agg_costs,
								   const AggClauseCosts *agg_partial_costs,
								   const AggClauseCosts *agg_final_costs)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	bool		has_ordered_aggs = agg_costs->numPureOrderedAggs > 0;
	cdb_agg_planning_context cxt;

	/* The caller should've checked these already */
	Assert(parse->hasAggs || parse->groupClause);
	/*
	 * This prohibition could be relaxed if we tracked missing combine
	 * functions per DQA and were willing to plan some DQAs as single and
	 * some as multiple phases.  Not currently, however.
	 */
	Assert(!agg_costs->hasNonCombine && !agg_costs->hasNonSerial);
	Assert(root->config->gp_enable_multiphase_agg);

	/* The caller already constructed a one-stage plan. */

	/*
	 * Ordered aggregates need to run the transition function on the
	 * values in sorted order, which in turn translates into single phase
	 * aggregation.
	 */
	if (has_ordered_aggs)
		return;

	/*
	 * We are currently unwilling to redistribute a gathered intermediate
	 * across the cluster.  This might change one day.
	 */
	if (!CdbPathLocus_IsPartitioned(cheapest_path->locus))
		return;

	memset(&cxt, 0, sizeof(cxt));
	cxt.target = target;
	cxt.partial_grouping_target = partial_grouping_target;
	cxt.dNumGroups = dNumGroups;
	cxt.agg_costs = agg_costs;
	cxt.agg_partial_costs = agg_partial_costs;
	cxt.agg_final_costs = agg_final_costs;

	/*
	 * Consider 2-phase aggs
	 */
	if (can_sort)
	{
		ListCell   *lc;

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);
			bool		is_sorted;

			is_sorted = pathkeys_contained_in(root->group_pathkeys,
											  path->pathkeys);
			if (path == cheapest_path || is_sorted)
			{
#if 0
				add_twostage_group_agg_path(root,
											path,
											is_sorted,
											&cxt,
											output_rel);
#endif
			}
		}
	}

	if (can_hash && list_length(agg_costs->distinctAggrefs) == 0)
	{
		add_twostage_hash_agg_path(root,
								   cheapest_path,
								   &cxt,
								   output_rel);
	}

	if ((can_hash || parse->groupClause == NIL) && list_length(agg_costs->distinctAggrefs) > 0)
	{
		/*
		 * Try possible plans for DISTINCT-qualified aggregate.
		 */

		cdb_distinct_info info = {};
		DQATYPE type = analyze_dqas(root, cheapest_path, &cxt, &info);
		switch (type)
		{
			case SINGLEDQA:
			{
				add_single_dqa_hash_agg_path(root,
				                             cheapest_path,
				                             &cxt,
				                             output_rel,
				                             info.input_target,
				                             info.dqa_group_clause);
			}
				break;
			case MULTIDQAS:
			{
#if 0
				CdbPathLocus distinct_locus;
				CdbPathLocus group_locus;
				bool		distinct_need_redistribute;
				bool        group_need_redistribute;
				Path*       path = cheapest_path;

				HashAggTableSizes hash_info;
				calcHashAggTableSizes(work_mem * 1024L,
				                      cxt.dNumGroups,
				                      path->pathtarget->width,
				                      false,	/* force */
				                      &hash_info);

				path = (Path *) create_projection_path(root, path->parent, path, input_target);

				group_locus = cdb_choose_grouping_locus(root, path,
				                                        input_target,
				                                        root->parse->groupClause, NIL, NIL,
				                                        &group_need_redistribute);

				distinct_locus = cdb_choose_grouping_locus(root, path,
				                                           input_target,
				                                           dqa_group_clause, NIL, NIL,
				                                           &distinct_need_redistribute);

				/* add SplitTupleId into pathtarget */
				input_target = copy_pathtarget(input_target);
				SplitTupleId *stid = makeNode(SplitTupleId);
				add_column_to_pathtarget(input_target, (Expr *)stid, 0);

				path = (Path *) create_agg_path(root,
				                                output_rel,
				                                path,
				                                input_target,
				                                AGG_SPLITORDER,
				                                AGGSPLIT_SIMPLE,
				                                false, /* streaming */
				                                dqa_group_clause,
				                                NIL,
				                                cxt.agg_partial_costs, /* FIXME */
				                                cxt.dNumGroups * getgpsegmentCount(),
				                                &hash_info);

				if (group_need_redistribute)
					path = cdbpath_create_motion_path(root, path, NIL, false,
					                                  group_locus);

				path = (Path *) create_agg_path(root,
				                                output_rel,
				                                path,
				                                cxt.target,
				                                parse->groupClause ? AGG_HASHED : AGG_PLAIN,
				                                AGGSPLIT_DEDUPLICATED,
				                                false, /* streaming */
				                                parse->groupClause,
				                                (List *) parse->havingQual,
				                                cxt.agg_final_costs,
				                                cxt.dNumGroups,
				                                &hash_info);

				add_path(output_rel, path);

				/*
				 * Comments just for debug split agg
				 */
#endif

#if 1
				add_multi_dqas_hash_agg_path(root,
				                            cheapest_path,
				                            &cxt,
				                            output_rel,
				                            &info);
#endif
			}
			break;
			case MIXEDDQAS:
				/* mixed dqas with normal aggregation does not support mpp path yet */
			default:
				break;
		}
	}
}

static void
add_twostage_group_agg_path(PlannerInfo *root,
							Path *path,
							bool is_sorted,
							cdb_agg_planning_context *ctx,
							RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
	Path	   *initial_agg_path;
	DQATYPE     dqa_type;
	CdbPathLocus group_locus;
	CdbPathLocus distinct_locus;
	CdbPathLocus singleQE_locus;
	bool		group_need_redistribute;
	bool		distinct_need_redistribute;

	group_locus = cdb_choose_grouping_locus(root, path, ctx->target,
											parse->groupClause, NIL, NIL,
											&group_need_redistribute);

	/*
	 * If the distribution of this path is suitable, two-stage aggregation
	 * is not applicable.
	 */
	if (!group_need_redistribute)
		return;

	/*
	 *  Finalize Aggregate
	 *    ->  Gather Motion
	 *        ->  Partial Aggregate
	 *            ->  Sort (if needed)
	 *                ->  Redistribute Motion (if multi DQAs)
	 *                    ->  SplitOrderAggregate (if multi DQAs)
	 *                      ->  Seq Scan
	 */
	if (ctx->agg_costs->distinctAggrefs)
	{
		cdb_distinct_info info = {};
		dqa_type = analyze_dqas(root, path, ctx, &info);

		if (dqa_type != SINGLEDQA && dqa_type != MULTIDQAS)
			return;

		path = (Path *) create_projection_path(root, path->parent, path, path->pathtarget);

		/* If the tuples are split, they are not distributed as before. */
		if (dqa_type == MULTIDQAS && CdbPathLocus_IsHashed(path->locus))
		{
			int numsegments = path->locus.numsegments;
			CdbPathLocus_MakeStrewn(&path->locus, numsegments);
		}
		distinct_locus = cdb_choose_grouping_locus(root, path,
												   info.input_target,
												   info.dqa_group_clause, NIL, NIL,
												   &distinct_need_redistribute);


		/* single DQA doesn't need to split */
		if (dqa_type == MULTIDQAS)
		{
			/* add SplitTupleId into pathtarget */
			SplitTupleId *stid = makeNode(SplitTupleId);
			add_column_to_pathtarget(info.input_target, (Expr *)stid, ++info.maxref);

			/* split the tuples if there is at least one dqa */
			path = (Path *) create_agg_path(root,
											output_rel,
											path,
											info.input_target,
											AGG_SPLITORDER,
											AGGSPLIT_SIMPLE,
											false, /* streaming */
											NIL,
											NIL,
											ctx->agg_partial_costs, /* FIXME */
											ctx->dNumGroups * getgpsegmentCount(),
											NULL);
		}

		if (distinct_need_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false, distinct_locus);
	}

	if (!is_sorted || dqa_type == MULTIDQAS || distinct_need_redistribute)
	{
		path = (Path *) create_sort_path(root,
										 output_rel,
										 path,
										 root->group_pathkeys,
										 -1.0);
	}

	initial_agg_path = (Path *) create_agg_path(root,
												output_rel,
												path,
												ctx->partial_grouping_target,
												parse->groupClause ? AGG_SORTED : AGG_PLAIN,
												AGGSPLIT_INITIAL_SERIAL,
												false, /* streaming */
												parse->groupClause,
												NIL,
												ctx->agg_partial_costs,
												ctx->dNumGroups * getgpsegmentCount(),
												NULL);

	/*
	 * GroupAgg -> GATHER MOTION -> GroupAgg.
	 *
	 * This has the advantage that it retains the input order. The
	 * downside is that it gathers everything to a single node. If that's
	 * where the final result is needed anyway, that's quite possibly better
	 * than scattering the partial aggregate results and having another
	 * motion to gather the final results, though,
	 *
	 * Alternatively, we could redistribute based on the GROUP BY key. That
	 * would have the advantage that the Finalize Agg stage could run in
	 * parallel. However, it would destroy the sort order, so it's probaly
	 * not a good idea.
	 */
	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root,
									  initial_agg_path,
									  initial_agg_path->pathkeys,
									  false,
									  singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->target,
									parse->groupClause ? AGG_SORTED : AGG_PLAIN,
									AGGSPLIT_FINAL_DESERIAL,
									false, /* streaming */
									parse->groupClause,
									(List *) parse->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroups,
									NULL);
	add_path(output_rel, path);
}

static void
add_twostage_hash_agg_path(PlannerInfo *root,
						   Path *path,
						   cdb_agg_planning_context *ctx,
						   RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
	Path	   *initial_agg_path;
	CdbPathLocus group_locus;
	bool		need_redistribute;
	HashAggTableSizes hash_info;

	group_locus = cdb_choose_grouping_locus(root, path, ctx->target,
											parse->groupClause, NIL, NIL,
											&need_redistribute);
	/*
	 * If the distribution of this path is suitable, two-stage aggregation
	 * is not applicable.
	 */
	if (!need_redistribute)
		return;

	if (!calcHashAggTableSizes(work_mem * 1024L,
							   ctx->dNumGroups,
							   path->pathtarget->width,
							   false,	/* force */
							   &hash_info))
		return;	/* don't try to hash */

	initial_agg_path = (Path *) create_agg_path(root,
												output_rel,
												path,
												ctx->partial_grouping_target,
												AGG_HASHED,
												AGGSPLIT_INITIAL_SERIAL,
												false, /* streaming */
												parse->groupClause,
												NIL,
												ctx->agg_partial_costs,
												ctx->dNumGroups * getgpsegmentCount(),
												&hash_info);

	/*
	 * HashAgg -> Redistribute or Gather Motion -> HashAgg.
	 */
	path = cdbpath_create_motion_path(root, initial_agg_path, NIL, false,
									  group_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->target,
									AGG_HASHED,
									AGGSPLIT_FINAL_DESERIAL,
									false, /* streaming */
									parse->groupClause,
									(List *) parse->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroups,
									&hash_info);
	add_path(output_rel, path);
}

static Node *
strip_aggdistinct_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref	   *newAggref = (Aggref *) copyObject(node);

		newAggref->aggdistinct = NIL;

		node = (Node *) newAggref;
	}
	return expression_tree_mutator(node, strip_aggdistinct_mutator, context);
}

static PathTarget *
strip_aggdistinct(PathTarget *target)
{
	PathTarget *result;

	result = copy_pathtarget(target);
	result->exprs = (List *) strip_aggdistinct_mutator((Node *) result->exprs, NULL);

	return result;
}

/*
 * analyze_dqas
 * fetch all mpp dqas path required information
 *
 * cdb_distinct_info is return value
 * info->input_target: subpath targetlist + all DISTINCT expr
 *
 *       dqa_group_clause: GROUP BY/ORDER BY clause + DISTINCT expr clause
 *
 *       distinct_ref: an array for only record DISTINCT expr reference index
 *       for splitorderagg, it require split tuple depends on each DISTINCT expr
 *       this array give a hint for that
 *
 *       maxref: after analyze the final maxref value
 *       if we want to add tuplesplitid expr, we need maxref to also add the expr
 *       into group by
 */
static DQATYPE
analyze_dqas(PlannerInfo *root,
			 Path *path,
			 cdb_agg_planning_context *ctx,
			 cdb_distinct_info *info)
{
	DQATYPE     ret = SINGLEDQA;
	PathTarget *input_target;
	ListCell   *lc;
	Index		maxRef;

	/* Prepare a modifiable copy of the input path target */
	input_target = copy_pathtarget(path->pathtarget);
	maxRef = 0;
	if (input_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(input_target->exprs); idx++)
		{
			if (input_target->sortgrouprefs[idx] > maxRef)
				maxRef = input_target->sortgrouprefs[idx];
		}
	}
	else
		input_target->sortgrouprefs = (Index *) palloc0(list_length(input_target->exprs) * sizeof(Index));

	/* Analyze the DISTINCT argument, to see if it's something we can
	 * support.
	 */
	foreach(lc, ctx->agg_costs->distinctAggrefs)
	{
		Aggref	   *aggref = (Aggref *) lfirst(lc);
		SortGroupClause *arg_sortcl;
		SortGroupClause *sortcl = NULL;
		TargetEntry *arg_tle;
		int			idx;
		ListCell   *lcc;

		if (list_length(aggref->aggdistinct) != 1)
			return INVALID;		/* I don't think the parser can even produce this */

		arg_sortcl = (SortGroupClause *) linitial(aggref->aggdistinct);
		arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

		if (!arg_sortcl->hashable)
		{
			/*
			 * XXX: I'm not sure if the hashable flag is always set correctly
			 * for DISTINCT args. DISTINCT aggs are never implemented with hashing
			 * in PostgreSQL.
			 */
			return INVALID;
		}

		/* Now find this expression in the sub-path's target list */
		idx = 0;
		foreach(lcc, input_target->exprs)
		{
			Expr		*expr = lfirst(lcc);

			if (equal(expr, arg_tle->expr))
				break;
			idx++;
		}

		/* if DISTINCT expr is not contained in input_target add it in otherwise just reference it */
		if (idx == list_length(input_target->exprs))
			add_column_to_pathtarget(input_target, arg_tle->expr, ++maxRef);
		else if (input_target->sortgrouprefs[idx] == 0)
			input_target->sortgrouprefs[idx] = ++maxRef;

		/* both side equal means, the expr is the first time reference */
		if(input_target->sortgrouprefs[idx] == maxRef)
		{
			sortcl = copyObject(arg_sortcl);
			sortcl->tleSortGroupRef = input_target->sortgrouprefs[idx];
			sortcl->hashable = true;	/* we verified earlier that it's hashable */

			info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);

		}


		if(!bms_is_member(input_target->sortgrouprefs[idx], info->dqas_ref_bm))
		{
			info->dqas_ref_bm = bms_add_member(info->dqas_ref_bm, input_target->sortgrouprefs[idx]);
			info->dqas_num ++;
		}
	}

	/* Check all DISTINCT on single expr */
	Index srtRf = 0;
	foreach(lc, info->dqa_group_clause)
	{
		SortGroupClause * srtcl = lfirst(lc);

		if(srtRf == 0)
			srtRf = srtcl->tleSortGroupRef;
		else if(srtRf != srtcl->tleSortGroupRef)
		{
			ret = MULTIDQAS;
			break;
		}
	}

	/* Check that there are no non-DISTINCT aggregates mixed in. */
	List *varnos = pull_var_clause((Node *) ctx->target->exprs,
	                               PVC_INCLUDE_AGGREGATES |
			                               PVC_INCLUDE_WINDOWFUNCS |
			                               PVC_INCLUDE_PLACEHOLDERS);
	foreach (lc, varnos)
	{
		Node	   *node = lfirst(lc);

		if (IsA(node, Aggref))
		{
			Aggref	   *aggref = (Aggref *) node;

			if (!aggref->aggdistinct)
			{
				/* mixing DISTINCT and non-DISTINCT aggs */
				ret = MIXEDDQAS;
				break;
			}
		}
	}


	info->input_target = input_target;
	info->dqa_group_clause = list_concat(
			list_copy(root->parse->groupClause),
			info->dqa_group_clause);
	info->maxref = maxRef;

	return ret;
}

/*
 * Create Paths for Multiple DISTINCT-qualified aggregates.
 */
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
                             Path *path,
                             cdb_agg_planning_context *cxt,
                             RelOptInfo *output_rel,
                             cdb_distinct_info *info)
{
	CdbPathLocus distinct_locus;
	CdbPathLocus group_locus;
	bool		distinct_need_redistribute;
	bool        group_need_redistribute;

	HashAggTableSizes hash_info;
	if (!calcHashAggTableSizes(work_mem * 1024L,
	                           cxt->dNumGroups,
	                           path->pathtarget->width,
	                           false,	/* force */
	                           &hash_info))
		return;


	/* Finalize Aggregate
	 *   -> Gather Motion
	 *        -> Partial Aggregate
	 *             -> HashAggregate, to remote duplicates
	 *                  -> Redistribute Motion (according to DISTINCT expr)
	 *                       -> Streaming SplitOrderAggregate (Split tuple according to DISTINCT expr)
	 *                            -> input
	 */
	path = (Path *) create_projection_path(root, path->parent, path, info->input_target);

	group_locus = cdb_choose_grouping_locus(root, path,
	                                        info->input_target,
	                                        root->parse->groupClause, NIL, NIL,
	                                        &group_need_redistribute);


	/* add SplitTupleId into pathtarget */
	{
		info->input_target = copy_pathtarget(info->input_target);
		SplitTupleId *stid = makeNode(SplitTupleId);
		add_column_to_pathtarget(info->input_target, (Expr *)stid, info->maxref);
	}

	/* add SplitTupleId into groupby clause */
	{
		Oid eqop;
		bool hashable;
		get_sort_group_operators(INT4OID, false, true, false, NULL, &eqop, NULL, &hashable);

		SortGroupClause *sortcl = makeNode(SortGroupClause);
		sortcl->tleSortGroupRef = info->maxref;
		sortcl->hashable = hashable;
		sortcl->eqop = eqop;

		info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
	}


	path = (Path *) create_split_agg_path(root,
	                                output_rel,
	                                path,
	                                info->input_target,
	                                AGG_SPLITORDER,
	                                AGGSPLIT_SIMPLE,
	                                true, /* streaming */
	                                root->parse->groupClause,
	                                NIL,
	                                cxt->agg_partial_costs, /* FIXME */
	                                cxt->dNumGroups * getgpsegmentCount(),
	                                &hash_info,
	                                info->dqas_ref_bm,
	                                info->dqas_num);


	distinct_locus = cdb_choose_grouping_locus(root, path,
	                                           info->input_target,
	                                           info->dqa_group_clause, NIL, NIL,
	                                           &distinct_need_redistribute);

	if(distinct_need_redistribute)
	path = cdbpath_create_motion_path(root, path, NIL, false,
	                                  distinct_locus);


	path = (Path *) create_agg_path(root,
	                                output_rel,
	                                path,
	                                info->input_target,
	                                AGG_HASHED,
	                                AGGSPLIT_SIMPLE,
	                                false, /* streaming */
	                                info->dqa_group_clause,
	                                NIL,
	                                cxt->agg_partial_costs, /* FIXME */
	                                cxt->dNumGroups * getgpsegmentCount(),
	                                &hash_info);

	path = (Path *) create_agg_path(root,
	                                output_rel,
	                                path,
	                                strip_aggdistinct(cxt->partial_grouping_target),
	                                root->parse->groupClause ? AGG_HASHED : AGG_PLAIN,
	                                AGGSPLIT_INITIAL_SERIAL | AGGSPLIT_DEDUPLICATED,
	                                false, /* streaming */
	                                root->parse->groupClause,
	                                NIL,
	                                cxt->agg_partial_costs,
	                                cxt->dNumGroups * getgpsegmentCount(),
	                                &hash_info);

	if (group_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
		                                  group_locus);

	path = (Path *) create_agg_path(root,
	                                output_rel,
	                                path,
	                                cxt->target,
	                                root->parse->groupClause ? AGG_HASHED : AGG_PLAIN,
	                                AGGSPLIT_FINAL_DESERIAL | AGGSPLIT_DEDUPLICATED,
	                                false, /* streaming */
	                                root->parse->groupClause,
	                                (List *) root->parse->havingQual,
	                                cxt->agg_final_costs,
	                                cxt->dNumGroups,
	                                &hash_info);

	add_path(output_rel, path);
}

/*
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate.
 */
static void
add_single_dqa_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 PathTarget *input_target,
							 List	   *dqa_group_clause)
{
	Query	   *parse = root->parse;
	CdbPathLocus group_locus;
	bool		group_need_redistribute;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;
	HashAggTableSizes hash_info;

	if (!gp_enable_agg_distinct)
		return;

	/*
	 * GPDB_96_MERGE_FIXME: compute the hash table size once. But we create
	 * several different Hash Aggs below, depending on the query. Is this
	 * computation sensible for all of them?
	 */
	if (!calcHashAggTableSizes(work_mem * 1024L,
							   ctx->dNumGroups,
							   path->pathtarget->width,
							   false,	/* force */
							   &hash_info))
		return;	/* don't try to hash */

	path = (Path *) create_projection_path(root, path->parent, path, input_target);

	distinct_locus = cdb_choose_grouping_locus(root, path,
											   input_target,
											   dqa_group_clause, NIL, NIL,
											   &distinct_need_redistribute);
	group_locus = cdb_choose_grouping_locus(root, path,
											input_target,
											parse->groupClause, NIL, NIL,
											&group_need_redistribute);
	if (!distinct_need_redistribute || ! group_need_redistribute)
	{
		/*
		 * 1. If the input's locus matches the DISTINCT, but not GROUP BY:
		 *
		 *  HashAggregate
		 *     -> Redistribute (according to GROUP BY)
		 *         -> HashAggregate (to eliminate duplicates)
		 *             -> input (hashed by GROUP BY + DISTINCT)
		 *
		 * 2. If the input's locus matches the GROUP BY:
		 *
		 *  HashAggregate (to aggregate)
		 *     -> HashAggregate (to eliminate duplicates)
		 *           -> input (hashed by GROUP BY)
		 *
		 * The main planner should already have created the single-stage
		 * Group Agg path.
		 *
		 * XXX: not sure if this makes sense. If hash distinct is a good
		 * idea, why doesn't PostgreSQL's agg node implement that?
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										ctx->dNumGroups * getgpsegmentCount(),
										&hash_info);

		if (group_need_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false,
											  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										(List *) parse->havingQual,
										ctx->agg_final_costs,
										ctx->dNumGroups,
										&hash_info);
		add_path(output_rel, path);
	}
	else if (CdbPathLocus_IsHashed(group_locus))
	{
		/*
		 *  HashAgg (to aggregate)
		 *     -> HashAgg (to eliminate duplicates)
		 *          -> Redistribute (according to GROUP BY)
		 *               -> Streaming HashAgg (to eliminate duplicates)
		 *                    -> input
		 *
		 * It may seem silly to have two Aggs on top of each other like this,
		 * but the Agg node can't do DISTINCT-aggregation by hashing at the
		 * moment. So we have to do it with two separate Aggs steps.
		 */
		if (gp_enable_dqa_pruning)
			path = (Path *) create_agg_path(root,
											output_rel,
											path,
											input_target,
											AGG_HASHED,
											AGGSPLIT_SIMPLE,
											true, /* streaming */
											dqa_group_clause,
											NIL,
											ctx->agg_partial_costs, /* FIXME */
											ctx->dNumGroups * getgpsegmentCount(),
											&hash_info);

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										ctx->dNumGroups * getgpsegmentCount(),
										&hash_info);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										(List *) parse->havingQual,
										ctx->agg_final_costs,
										ctx->dNumGroups,
										&hash_info);
		add_path(output_rel, path);
	}
	else if (CdbPathLocus_IsHashed(distinct_locus))
	{
		/*
		 *  Finalize Aggregate
		 *     -> Gather Motion
		 *          -> Partial Aggregate
		 *              -> HashAggregate, to remove duplicates
		 *                  -> Redistribute Motion (according to DISTINCT arg)
		 *                      -> Streaming HashAgg (to eliminate duplicates)
		 *                          -> input
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										true, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										ctx->dNumGroups * getgpsegmentCount(),
										&hash_info);

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										ctx->dNumGroups * getgpsegmentCount(),
										&hash_info);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										strip_aggdistinct(ctx->partial_grouping_target),
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_INITIAL_SERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										NIL,
										ctx->agg_partial_costs,
										ctx->dNumGroups * getgpsegmentCount(),
										&hash_info);
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_FINAL_DESERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										(List *) parse->havingQual,
										ctx->agg_final_costs,
										ctx->dNumGroups,
										&hash_info);

		add_path(output_rel, path);
	}
	else
		return;
}

/*
 * Figure out the desired data distribution to perform the grouping.
 *
 * In case of a simple GROUP BY, we prefer to distribute the data according to
 * the GROUP BY. With multiple grouping sets, identify the set of common
 * entries, and distribute based on that. For example, if you do
 * GROUP BY GROUPING SETS ((a, b, c), (b, c)), the common cols are b and c.
 */
CdbPathLocus
cdb_choose_grouping_locus(PlannerInfo *root, Path *path,
					  PathTarget *target,
					  List *groupClause,
					  List *rollup_lists,
					  List *rollup_groupclauses,
					  bool *need_redistribute_p)
{
	List	   *tlist = make_tlist_from_pathtarget(target);
	CdbPathLocus locus;
	bool		need_redistribute;

	/*
	 * If the input is already collected to a single segment, just perform the
	 * aggregation there. We could redistribute it, so that we could perform
	 * the aggregation in parallel, but Motions are pretty expensive so it's
	 * probably not worthwhile.
	 */
	if (CdbPathLocus_IsBottleneck(path->locus))
	{
		need_redistribute = false;
		CdbPathLocus_MakeNull(&locus, getgpsegmentCount());
	}
	else
	{
		List	   *group_tles;
		List	   *hash_exprs;
		List	   *hash_opfamilies;
		List	   *hash_sortrefs;
		ListCell   *lc;
		Bitmapset  *common_groupcols = NULL;
		bool		first = true;
		int			x;

		if (rollup_lists)
		{
			ListCell   *lcl, *lcc;

			forboth(lcl, rollup_lists, lcc, rollup_groupclauses)
			{
				List *rlist = (List *) lfirst(lcl);
				List *rclause = (List *) lfirst(lcc);
				List *last_list = (List *) llast(rlist);
				Bitmapset *this_groupcols = NULL;

				this_groupcols = NULL;
				foreach (lc, last_list)
				{
					SortGroupClause *sc = list_nth(rclause, lfirst_int(lc));

					this_groupcols = bms_add_member(this_groupcols, sc->tleSortGroupRef);
				}

				if (first)
					common_groupcols = this_groupcols;
				else
				{
					common_groupcols = bms_int_members(common_groupcols, this_groupcols);
					bms_free(this_groupcols);
				}
				first = false;
			}
		}
		else
		{
			foreach(lc, groupClause)
			{
				SortGroupClause *sc = lfirst(lc);

				common_groupcols = bms_add_member(common_groupcols, sc->tleSortGroupRef);
			}
		}

		x = -1;
		group_tles = NIL;
		while ((x = bms_next_member(common_groupcols, x)) >= 0)
		{
			TargetEntry *tle = get_sortgroupref_tle(x, tlist);

			group_tles = lappend(group_tles, tle);
		}

		if (!group_tles)
			need_redistribute = true;
		else
			need_redistribute = !cdbpathlocus_is_hashed_on_tlist(path->locus, group_tles, true);

		hash_exprs = NIL;
		hash_opfamilies = NIL;
		hash_sortrefs = NIL;
		foreach(lc, group_tles)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Oid			typeoid = exprType((Node *) tle->expr);
			Oid			opfamily;
			Oid			eqopoid;

			opfamily = cdb_default_distribution_opfamily_for_type(typeoid);
			if (!OidIsValid(opfamily))
				continue;

			/*
			 * If the datatype isn't mergejoinable, then we cannot represent
			 * the grouping in the locus. Skip such expressions.
			 */
			eqopoid = cdb_eqop_in_hash_opfamily(opfamily, typeoid);
			if (!op_mergejoinable(eqopoid, typeoid))
				continue;

			hash_exprs = lappend(hash_exprs, tle->expr);
			hash_opfamilies = lappend_oid(hash_opfamilies, opfamily);
			hash_sortrefs = lappend_int(hash_sortrefs, tle->ressortgroupref);
		}

		if (need_redistribute)
		{
			if (hash_exprs)
				locus = cdbpathlocus_from_exprs(root, hash_exprs, hash_opfamilies, hash_sortrefs, getgpsegmentCount());
			else
				CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
		}
		else
			CdbPathLocus_MakeNull(&locus, getgpsegmentCount());
	}

	*need_redistribute_p = need_redistribute;
	return locus;
}