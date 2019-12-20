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
#include "optimizer/clauses.h"

typedef enum
{
	INVALID_DQA = -1,
	SINGLE_DQA, /* only contain an unique DQA expr */
	MULTI_DQAS, /* contain multiple DQA exprs */
	MIXED_DQAS  /* mixed DQA and Agg */
} DQAType;

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
    PathTarget *target;                     /* finalize agg tlist */
    PathTarget *partial_grouping_target;    /* partial agg tlist */
    PathTarget *input_target;               /* AggExprId + subpath_proj_target */
    PathTarget *subpath_proj_target;        /* input tuple tlist + DQA expr */

	List       *dqa_group_clause;

	int         dqas_num;                   /* dqas_ref_bm size */
	Bitmapset  *dqas_ref_bm;                /* DQA expr sortgroupref bitmap */

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
										 List       *dqa_group_clause);

static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 cdb_distinct_info *info);

static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_distinct_info *info);

static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_distinct_info *info);

static DQAType
recognise_dqa_type(PlannerInfo *root,
				   Path *path,
				   cdb_agg_planning_context *ctx);

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
				add_twostage_group_agg_path(root,
											path,
											is_sorted,
											&cxt,
											output_rel);
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
		DQAType type = recognise_dqa_type(root, cheapest_path, &cxt);
		switch (type)
		{
			case SINGLE_DQA:
			{
				fetch_single_dqa_info(root, cheapest_path, &cxt, &info);

				add_single_dqa_hash_agg_path(root,
											 cheapest_path,
											 &cxt,
											 output_rel,
											 info.input_target,
											 info.dqa_group_clause);
			}
				break;
			case MULTI_DQAS:
			{
				fetch_multi_dqas_info(root, cheapest_path, &cxt, &info);

				add_multi_dqas_hash_agg_path(root,
											 cheapest_path,
											 &cxt,
											 output_rel,
											 &info);
			}
				break;
			case MIXED_DQAS:
				/*
				 * GPDB_96_MERGE_FIXME: MIXED DISTINCT-Qualified Aggregates and Aggregagtes Path
				 * not re-implemented yet
				 */
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
	CdbPathLocus singleQE_locus;
	Path	   *initial_agg_path;
	DQAType     dqa_type;
	CdbPathLocus group_locus;
	bool		need_redistribute;

	group_locus = cdb_choose_grouping_locus(root, path, ctx->target,
											parse->groupClause, NIL, NIL,
											&need_redistribute);
	/*
	 * If the distribution of this path is suitable, two-stage aggregation
	 * is not applicable.
	 */
	if (!need_redistribute)
		return;

	if (ctx->agg_costs->distinctAggrefs)
	{
		cdb_distinct_info info = {};
		CdbPathLocus distinct_locus;

		bool		distinct_need_redistribute;

		dqa_type = recognise_dqa_type(root, path, ctx);

		if (dqa_type != SINGLE_DQA)
			return;

		fetch_single_dqa_info(root, path, ctx, &info);

		/*
		 * If subpath is projection capable, we do not want to generate
		 * projection plan. The reason is that the projection plan does not
		 * constrain child tlist when it create subplan. Thus, GROUP BY expr
		 * may not find in scan targetlist.
		 */
		path = apply_projection_to_path(root, path->parent, path, info.input_target);

		distinct_locus = cdb_choose_grouping_locus(root, path,
												   info.input_target,
												   info.dqa_group_clause, NIL, NIL,
												   &distinct_need_redistribute);

		/* If the input distribution matches the distinct, we can proceed */
		if (distinct_need_redistribute)
			return;

	}

	if (!is_sorted)
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
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate.
 */
static void
add_single_dqa_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 PathTarget *input_target,
							 List       *dqa_group_clause)
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

	/*
	 * If subpath is projection capable, we do not want to generate
	 * projection plan. The reason is that the projection plan does not
	 * constrain child tlist when it create subplan. Thus, GROUP BY expr
	 * may not find in scan targetlist.
	 */
	 path = apply_projection_to_path(root, path->parent, path, input_target);

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
 * Create Paths for Multiple DISTINCT-qualified aggregates.
 *
 * For us target is that using single execution path to handle all DQAs, so
 * before remove duplication a SplitTuple node is created. This node handle
 * each input tuple to n output tuples(n is DQA expr number). Each output tuple
 * only contain an AggExprId, one DQA expr and all GROUP by expr . For example,
 * SELECT DQA(a), DQA(b) FROM foo GROUP BY c;
 * After the tuple split, two tuples are generated:
 * -------------------
 * | 1 | a | n/a | c |
 * -------------------
 * -------------------
 * | 2 | n/a | b | c |
 * -------------------
 *
 * In an aggregate executor, if input tuple contain AggExprId, that means the tuple is
 * splited, checking AggExprId's value is match Aggref expr.
 */
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *cxt,
							 RelOptInfo *output_rel,
							 cdb_distinct_info *info)
{
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;

	HashAggTableSizes hash_info;
	if (!calcHashAggTableSizes(work_mem * 1024L,
							   cxt->dNumGroups,
							   path->pathtarget->width,
							   false,	/* force */
							   &hash_info))
		return;
	/*
	 * If subpath is projection capable, we do not want to generate
	 * projection plan. The reason is that the projection plan does not
	 * constrain child tlist when it create subplan. Thus, GROUP BY expr
	 * may not find in scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, info->subpath_proj_target);

	/*
	 * Finalize Aggregate
	 *   -> Gather Motion
	 *        -> Partial Aggregate
	 *             -> HashAggregate, to remote duplicates
	 *                  -> Redistribute Motion
	 *                       -> Split Ordered Aggregate  (Split tuple according to DISTINCT expr)
	 *                            -> input
	 */

	path = (Path *)create_tup_split_path(root,
										 output_rel,
										 path,
										 info->input_target,
										 root->parse->groupClause,
										 info->dqas_ref_bm,
										 info->dqas_num);

	AggClauseCosts DedupCost = {};
    get_agg_clause_costs(root, (Node *) info->input_target->exprs,
                         AGGSPLIT_SIMPLE,
                         &DedupCost);


	if (gp_enable_dqa_pruning)
    {
        path = (Path *) create_agg_path(root,
                                        output_rel,
                                        path,
                                        info->input_target,
                                        AGG_HASHED,
                                        AGGSPLIT_SIMPLE,
                                        true, /* streaming */
                                        NIL,
                                        NIL,
                                        &DedupCost,
                                        cxt->dNumGroups * getgpsegmentCount(),
                                        &hash_info);

        /*
         * FIXME: set group clause after path create is for cheating current cost model.
         * Upstream cost calculation is not totally suitable in MPP, the MULTIDQA result
         * sometime is larger than single node Aggregation. It must be wrong. We should
         * have gpdb private cost method.
         */
        ((AggPath *)path)->groupClause = info->dqa_group_clause;
    }

	distinct_locus = cdb_choose_grouping_locus(root, path,
											   info->input_target,
											   info->dqa_group_clause, NIL, NIL,
											   &distinct_need_redistribute);

	if (distinct_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);


	AggStrategy split = AGG_PLAIN;
	unsigned long DEDUPLICATED_FLAG = 0;
	PathTarget *partial_target = info->partial_grouping_target;

	if (root->parse->groupClause)
	{
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										info->input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										info->dqa_group_clause,
										NIL,
										&DedupCost,
										cxt->dNumGroups * getgpsegmentCount(),
										&hash_info);

		split = AGG_HASHED;
		DEDUPLICATED_FLAG = AGGSPLITOP_DEDUPLICATED ;
		partial_target = strip_aggdistinct(info->partial_grouping_target);
	}

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									partial_target,
									split,
									AGGSPLIT_INITIAL_SERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									root->parse->groupClause,
									NIL,
									cxt->agg_partial_costs,
									cxt->dNumGroups * getgpsegmentCount(),
									&hash_info);

	CdbPathLocus singleQE_locus;
	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root,
									  path,
									  NIL,
									  false,
									  singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									info->target,
									split,
									AGGSPLIT_FINAL_DESERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									root->parse->groupClause,
									(List *) root->parse->havingQual,
									cxt->agg_final_costs,
									cxt->dNumGroups,
									&hash_info);

	add_path(output_rel, path);
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

static void
ref_dqa_expr(cdb_distinct_info *info, SortGroupClause *arg_sortcl, int ref)
{
	SortGroupClause *sortcl;

	sortcl = copyObject(arg_sortcl);
	sortcl->tleSortGroupRef = ref;
	sortcl->hashable = true;	/* we verified earlier that it's hashable */

	info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);

	info->dqas_ref_bm = bms_add_member(info->dqas_ref_bm, ref);
	info->dqas_num ++;
}

static DQAType
recognise_dqa_type(PlannerInfo *root,
				   Path *path,
				   cdb_agg_planning_context *ctx)
{
	ListCell   *lc;
	Expr    *dqaExpr = NULL;
	DQAType ret;

	ret = INVALID_DQA;

	foreach (lc, ctx->agg_costs->distinctAggrefs)
	{
		Aggref *aggref = (Aggref *) lfirst(lc);
		SortGroupClause *arg_sortcl;
		TargetEntry *arg_tle;

		if (list_length(aggref->aggdistinct) != 1)
			return ret;        /* I don't think the parser can even produce this */

		arg_sortcl = (SortGroupClause *) linitial(aggref->aggdistinct);
		arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

		if (!arg_sortcl->hashable)
		{
			/*
			 * XXX: I'm not sure if the hashable flag is always set correctly
			 * for DISTINCT args. DISTINCT aggs are never implemented with hashing
			 * in PostgreSQL.
			 */
			return ret;
		}

		if (dqaExpr == NULL)
		{
			dqaExpr = arg_tle->expr;
			ret = SINGLE_DQA;
		}
		else if (!equal(dqaExpr, arg_tle->expr))
		{
			ret = MULTI_DQAS;
			break;
		}
	}

	if (ret != INVALID_DQA)
	{
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
					return MIXED_DQAS;
				}
			}
		}
	}

	return ret;
}

/*
 * fetch_multi_dqas_info
 * 1. fetch all dqas path required information as single dqa's function.
 *
 * 2. Appending an AggExprId into Pathtarget to indicate which DQA expr
 * contains in the output tuple after TupleSplit.
 *
 */
static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_distinct_info *info)
{
    Index		maxRef;
	ListCell    *lc;
	PathTarget *ctarget = copy_pathtarget(ctx->target);
	PathTarget *cpartial_grouping_target = copy_pathtarget(ctx->partial_grouping_target);

	info->input_target = copy_pathtarget(path->pathtarget);
    info->subpath_proj_target = copy_pathtarget(path->pathtarget);

    /* Give a hint for Aggregation which agg expr remain in tuple */
    {
        AggExprId *a_expr_id = makeNode(AggExprId);
        info->input_target->exprs = lcons(a_expr_id, info->input_target->exprs);
        info->input_target->sortgrouprefs = palloc0((list_length(path->pathtarget->exprs) + 1) * sizeof(Index));
    }

    /* Prepare a modifiable copy of the input path target */
	maxRef = 0;
	if (path->pathtarget->sortgrouprefs)
	{
        memcpy(info->input_target->sortgrouprefs + 1,
               path->pathtarget->sortgrouprefs,
               sizeof(list_length(path->pathtarget->exprs) * sizeof(Index)));

		for (int idx = 0; idx < list_length(info->input_target->exprs); idx++)
		{
			if (info->input_target->sortgrouprefs[idx] > maxRef)
				maxRef = info->input_target->sortgrouprefs[idx];
		}
	}
	else
    {
	    info->subpath_proj_target->sortgrouprefs = palloc0(list_length(path->pathtarget->exprs) * sizeof(Index));
    }

	foreach(lc, ctx->agg_costs->distinctAggrefs)
	{
		Aggref	   *aggref = (Aggref *) lfirst(lc);
		SortGroupClause *arg_sortcl;
		TargetEntry *arg_tle;
		ListCell   *lcc;

		arg_sortcl = (SortGroupClause *) linitial(aggref->aggdistinct);
		arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

		ListCell *dqa_lc = NULL;
		int dqa_lc_idx;

		/* find DQA expr and its ShadowExpr in PathTarget list */
		int			idx = 0;
		foreach (lcc, info->input_target->exprs)
		{
			Expr		*expr = lfirst(lcc);

			if (equal(arg_tle->expr, expr))
			{
				dqa_lc = lcc;
				dqa_lc_idx = idx;
                break;
			}

			idx ++;
		}

        if (!dqa_lc)
        {
            /*
             * DQA expr does not in PathTarget
             *
             * SELECT DQA( a + 1 ) FROM foo;
             */
            add_column_to_pathtarget(info->input_target, arg_tle->expr, ++maxRef);
            add_column_to_pathtarget(info->subpath_proj_target, arg_tle->expr, maxRef);
            ref_dqa_expr(info, arg_sortcl, maxRef);
        }
        else if (dqa_lc && info->input_target->sortgrouprefs[dqa_lc_idx] == 0)
        {
            /*
             * DQA expr in PathTarget but no reference
             *
             * SELECT DQA(a) FROM foo;
             */
            info->input_target->sortgrouprefs[dqa_lc_idx] = ++maxRef;
            info->subpath_proj_target->sortgrouprefs[dqa_lc_idx - 1] = maxRef;
            ref_dqa_expr(info, arg_sortcl, maxRef);
        }
        else
        {
            /*
             * DQA expr in PathTarget and referenced by GROUP BY clause
             *
             * SELECT DQA(a) FROM foo GROUP BY a;
             */
            Index exprRef = info->input_target->sortgrouprefs[dqa_lc_idx];
            info->dqas_ref_bm = bms_add_member(info->dqas_ref_bm, exprRef);
            info->dqas_num ++;
        }
    }

	/* add AggExprId into GROUPBY clause */
    {
        Oid eqop;
        bool hashable;
        get_sort_group_operators(INT4OID, false, true, false, NULL, &eqop, NULL, &hashable);

        SortGroupClause *sortcl = makeNode(SortGroupClause);
        sortcl->tleSortGroupRef = ++maxRef;
        sortcl->hashable = hashable;
        sortcl->eqop = eqop;
        info->dqa_group_clause = lcons(sortcl, info->dqa_group_clause);
        info->input_target->sortgrouprefs[0] = sortcl->tleSortGroupRef;
    }

    info->dqa_group_clause = list_concat(info->dqa_group_clause,
                                         list_copy(root->parse->groupClause));
	info->target = ctarget;
	info->partial_grouping_target = cpartial_grouping_target;
}

/*
 * fetch_single_dqa_info
 *
 * fetch single dqa path required information and store in cdb_distinct_info
 *
 * info->input_target contain subpath target expr + all DISTINCT expr
 *
 * info->dqa_group_clause contain DISTINCT expr + GROUP BY expr
 *
 */
static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_distinct_info *info)
{
	Index		maxRef;

	/* Prepare a modifiable copy of the input path target */
	info->input_target = copy_pathtarget(path->pathtarget);
	maxRef = 0;
	if (info->input_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(info->input_target->exprs); idx++)
		{
			if (info->input_target->sortgrouprefs[idx] > maxRef)
				maxRef = info->input_target->sortgrouprefs[idx];
		}
	}
	else
		info->input_target->sortgrouprefs = (Index *) palloc0(list_length(info->input_target->exprs) * sizeof(Index));

	Aggref	   *aggref = list_nth(ctx->agg_costs->distinctAggrefs, 0);
	SortGroupClause *arg_sortcl;
	SortGroupClause *sortcl = NULL;
	TargetEntry *arg_tle;
	int			idx = 0;
	ListCell   *lcc;

	arg_sortcl = (SortGroupClause *) linitial(aggref->aggdistinct);
	arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

	/* Now find this expression in the sub-path's target list */
	idx = 0;
	foreach(lcc, info->input_target->exprs)
	{
		Expr		*expr = lfirst(lcc);

		if (equal(expr, arg_tle->expr))
			break;
		idx++;
	}

	if (idx == list_length(info->input_target->exprs))
		add_column_to_pathtarget(info->input_target, arg_tle->expr, ++maxRef);
	else if (info->input_target->sortgrouprefs[idx] == 0)
		info->input_target->sortgrouprefs[idx] = ++maxRef;

	sortcl = copyObject(arg_sortcl);
	sortcl->tleSortGroupRef = info->input_target->sortgrouprefs[idx];
	sortcl->hashable = true;	/* we verified earlier that it's hashable */

	info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);

	info->dqa_group_clause = list_concat(list_copy(root->parse->groupClause),
										 info->dqa_group_clause);
}
