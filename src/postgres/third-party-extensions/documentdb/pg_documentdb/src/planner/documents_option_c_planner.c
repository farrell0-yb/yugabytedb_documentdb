/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/planner/documents_option_c_planner.c
 *
 * Adds a narrow planner path that can satisfy eligible DocumentDB BSON equality
 * filters by driving from an Option C ic_<collection_id>_<index_id> side table.
 *
 * This is deliberately conservative.  It only creates a path when all extracted
 * predicates are scalar constants that match a valid Option C index prefix.  The
 * original BSON quals are left on the CustomScan plan as a runtime recheck, so
 * unsupported semantics fall back to the normal planner instead of changing
 * query results.
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>

#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <catalog/pg_collation.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <executor/spi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "io/bson_core.h"
#include "metadata/collection.h"
#include "metadata/metadata_cache.h"
#include "planner/documents_option_c_planner.h"

#define OPTION_C_MAX_PLANNER_FIELDS 4

#define OPTION_C_VALUE_NUMERIC 'n'
#define OPTION_C_VALUE_TEXT 't'

typedef struct OptionCPlanField
{
	int column;
	char valueKind;
	char *valueText;
} OptionCPlanField;

typedef struct OptionCExtractedQuals
{
	int nfields;
	char *fieldNames[OPTION_C_MAX_PLANNER_FIELDS];
	OptionCPlanField fields[OPTION_C_MAX_PLANNER_FIELDS];
} OptionCExtractedQuals;


#define OptionCPrivateNodeName "DocumentDBOptionCPrivate"

typedef struct OptionCPrivate
{
	ExtensibleNode extensible;
	int collectionId;
	int indexId;
	int nfields;
	int columns[OPTION_C_MAX_PLANNER_FIELDS];
	int kinds[OPTION_C_MAX_PLANNER_FIELDS];
	char *values[OPTION_C_MAX_PLANNER_FIELDS];
} OptionCPrivate;

static void CopyOptionCPrivateNode(ExtensibleNode *target_node,
								  const ExtensibleNode *source_node);
static void OutOptionCPrivateNode(StringInfo str, const ExtensibleNode *raw_node);
static void ReadOptionCPrivateNode(ExtensibleNode *node);
static bool EqualOptionCPrivateNode(const ExtensibleNode *a, const ExtensibleNode *b);
static void RegisterOptionCPrivateNode(void);

static const ExtensibleNodeMethods OptionCPrivateMethods =
{
	OptionCPrivateNodeName,
	sizeof(OptionCPrivate),
	CopyOptionCPrivateNode,
	EqualOptionCPrivateNode,
	OutOptionCPrivateNode,
	ReadOptionCPrivateNode
};

static void
RegisterOptionCPrivateNode(void)
{
	static bool registered = false;
	if (!registered)
	{
		RegisterExtensibleNodeMethods(&OptionCPrivateMethods);
		registered = true;
	}
}

static void
CopyOptionCPrivateNode(ExtensibleNode *target_node, const ExtensibleNode *source_node)
{
	OptionCPrivate *from = (OptionCPrivate *) source_node;
	OptionCPrivate *to = (OptionCPrivate *) target_node;
	to->extensible.type = T_ExtensibleNode;
	to->extensible.extnodename = OptionCPrivateNodeName;
	to->collectionId = from->collectionId;
	to->indexId = from->indexId;
	to->nfields = from->nfields;
	for (int i = 0; i < from->nfields; i++)
	{
		to->columns[i] = from->columns[i];
		to->kinds[i] = from->kinds[i];
		to->values[i] = pstrdup(from->values[i]);
	}
}

static void
OutOptionCPrivateNode(StringInfo str, const ExtensibleNode *raw_node)
{
	OptionCPrivate *node = (OptionCPrivate *) raw_node;
	appendStringInfo(str, " :collectionId %d :indexId %d :nfields %d",
				 node->collectionId, node->indexId, node->nfields);
	for (int i = 0; i < node->nfields; i++)
	{
		appendStringInfo(str, " :c%d %d :k%d %d :v%d ", i, node->columns[i],
					 i, node->kinds[i], i);
		outToken(str, node->values[i]);
	}
}

static void
ReadOptionCPrivateNode(ExtensibleNode *node)
{
	ereport(ERROR, (errmsg("Read for node type DocumentDBOptionCPrivate not implemented")));
}

static bool
EqualOptionCPrivateNode(const ExtensibleNode *a, const ExtensibleNode *b)
{
	return false;
}

typedef struct OptionCScanState
{
	CustomScanState custom_scanstate;
	bool spiConnected;
	SPITupleTable *tuptable;  /* unused during scan; kept for API compat */
	HeapTuple *rows;          /* result rows copied into executor memory */
	TupleDesc  tupdesc;       /* descriptor for rows[] tuples */
	uint64 processed;
	uint64 nextRow;
} OptionCScanState;

static Plan *OptionCPlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
								   struct CustomPath *bestPath, List *tlist,
								   List *clauses, List *customPlans);
static Node *OptionCCreateCustomScanState(CustomScan *cscan);
static void OptionCBeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *OptionCExecCustomScan(CustomScanState *node);
static TupleTableSlot *OptionCNext(CustomScanState *node);
static bool OptionCRecheck(ScanState *state, TupleTableSlot *slot);
static void OptionCEndCustomScan(CustomScanState *node);
static void OptionCReScanCustomScan(CustomScanState *node);
static void OptionCExplainCustomScan(CustomScanState *node, List *ancestors,
									ExplainState *es);

static const struct CustomPathMethods OptionCPathMethods = {
	.CustomName = "DocumentDBOptionCScan",
	.PlanCustomPath = OptionCPlanCustomPath,
};

static const struct CustomScanMethods OptionCScanMethods = {
	.CustomName = "DocumentDBOptionCScan",
	.CreateCustomScanState = OptionCCreateCustomScanState,
};

static const struct CustomExecMethods OptionCExecMethods = {
	.CustomName = "DocumentDBOptionCScan",
	.BeginCustomScan = OptionCBeginCustomScan,
	.ExecCustomScan = OptionCExecCustomScan,
	.EndCustomScan = OptionCEndCustomScan,
	.ReScanCustomScan = OptionCReScanCustomScan,
	.ExplainCustomScan = OptionCExplainCustomScan,
};


static Const *
OptionCMakeIntConst(int value)
{
	return makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(value),
				 false, true);
}

static Const *
OptionCMakeTextConst(const char *value)
{
	return makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
				 CStringGetTextDatum(value), false, false);
}

static int
OptionCIntFromConst(List *items, int index)
{
	Const *value = (Const *) list_nth(items, index);
	return DatumGetInt32(value->constvalue);
}

static char *
OptionCTextFromConst(List *items, int index)
{
	Const *value = (Const *) list_nth(items, index);
	return TextDatumGetCString(value->constvalue);
}

static bool
OptionCExtractValue(const bson_value_t *value, OptionCPlanField *field)
{
	field->valueText = NULL;

	switch (value->value_type)
	{
		case BSON_TYPE_INT32:
			field->valueKind = OPTION_C_VALUE_NUMERIC;
			field->valueText = psprintf("%d", value->value.v_int32);
			return true;
		case BSON_TYPE_INT64:
			field->valueKind = OPTION_C_VALUE_NUMERIC;
			field->valueText = psprintf(INT64_FORMAT, value->value.v_int64);
			return true;
		case BSON_TYPE_DOUBLE:
			field->valueKind = OPTION_C_VALUE_NUMERIC;
			field->valueText = psprintf("%.17g", value->value.v_double);
			return true;
		case BSON_TYPE_BOOL:
			field->valueKind = OPTION_C_VALUE_NUMERIC;
			field->valueText = pstrdup(value->value.v_bool ? "1" : "0");
			return true;
		case BSON_TYPE_UTF8:
			field->valueKind = OPTION_C_VALUE_TEXT;
			field->valueText = pnstrdup(value->value.v_utf8.str,
									 value->value.v_utf8.len);
			return true;
		case BSON_TYPE_OID:
		{
			char oidHex[25];
			bson_oid_to_string(&value->value.v_oid, oidHex);
			field->valueKind = OPTION_C_VALUE_TEXT;
			field->valueText = pstrdup(oidHex);
			return true;
		}
		default:
			return false;
	}
}

static bool
OptionCExtractEqualityQual(RestrictInfo *rinfo, OptionCExtractedQuals *quals)
{
	if (!IsA(rinfo->clause, OpExpr))
	{
		return false;
	}

	OpExpr *opExpr = (OpExpr *) rinfo->clause;
	if (list_length(opExpr->args) != 2)
	{
		return false;
	}

	if (opExpr->opno != BsonEqualMatchOperatorId() &&
		opExpr->opno != BsonEqualMatchRuntimeOperatorId())
	{
		return false;
	}

	Expr *leftExpr = linitial(opExpr->args);
	Expr *rightExpr = lsecond(opExpr->args);
	if (!IsA(leftExpr, Var) || !IsA(rightExpr, Const))
	{
		return false;
	}

	Var *leftVar = (Var *) leftExpr;
	if (leftVar->varattno != DOCUMENT_DATA_TABLE_DOCUMENT_VAR_ATTR_NUMBER)
	{
		return false;
	}

	Const *rightConst = (Const *) rightExpr;
	if (rightConst->constisnull)
	{
		return false;
	}

	pgbsonelement element = { 0 };
	if (!TryGetSinglePgbsonElementFromPgbson(DatumGetPgBsonPacked(rightConst->constvalue),
										  &element))
	{
		return false;
	}

	if (element.path == NULL || element.pathLength <= 0 || strchr(element.path, '.') != NULL)
	{
		return false;
	}

	for (int i = 0; i < quals->nfields; i++)
	{
		if (strcmp(quals->fieldNames[i], element.path) == 0)
		{
			return false;
		}
	}

	if (quals->nfields >= OPTION_C_MAX_PLANNER_FIELDS)
	{
		return false;
	}

	OptionCPlanField field = { 0 };
	if (!OptionCExtractValue(&element.bsonValue, &field))
	{
		return false;
	}

	quals->fieldNames[quals->nfields] = pstrdup(element.path);
	quals->fields[quals->nfields] = field;
	quals->nfields++;
	return true;
}

static bool
OptionCLookupIndex(uint64 collectionId, OptionCExtractedQuals *quals, int32 *indexId)
{
	if (quals->nfields <= 0)
	{
		return false;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("option_c planner: could not connect to SPI")));
	}

	StringInfoData sql;
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT index_id, field_paths FROM %s.option_c_indexes "
					 "WHERE collection_id = $1 AND is_valid "
					 "AND cardinality(field_paths) >= %d "
					 "AND field_paths[1:%d] = ARRAY[",
					 ApiCatalogSchemaName, quals->nfields, quals->nfields);
	for (int i = 0; i < quals->nfields; i++)
	{
		if (i > 0)
		{
			appendStringInfoString(&sql, ",");
		}
		appendStringInfo(&sql, "$%d", i + 2);
	}
	appendStringInfoString(&sql, "]::text[] LIMIT 1");

	int nargs = quals->nfields + 1;
	Oid *argTypes = palloc0(sizeof(Oid) * nargs);
	Datum *argValues = palloc0(sizeof(Datum) * nargs);
	char *nulls = palloc0(sizeof(char) * nargs);

	argTypes[0] = INT8OID;
	argValues[0] = Int64GetDatum((int64) collectionId);
	nulls[0] = ' ';
	for (int i = 0; i < quals->nfields; i++)
	{
		argTypes[i + 1] = TEXTOID;
		argValues[i + 1] = CStringGetTextDatum(quals->fieldNames[i]);
		nulls[i + 1] = ' ';
	}

	int spiStatus = SPI_execute_with_args(sql.data, nargs, argTypes, argValues,
									 nulls, true, 1);
	pfree(sql.data);

	if (spiStatus != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return false;
	}

	bool isNull = false;
	Datum indexDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								  1, &isNull);
	if (isNull)
	{
		SPI_finish();
		return false;
	}

	*indexId = DatumGetInt32(indexDatum);
	for (int i = 0; i < quals->nfields; i++)
	{
		quals->fields[i].column = i;
	}

	SPI_finish();
	return true;
}

static List *
OptionCBuildPrivate(uint64 collectionId, int32 indexId, OptionCExtractedQuals *quals)
{
	OptionCPrivate *private = palloc0(sizeof(OptionCPrivate));
	private->extensible.type = T_ExtensibleNode;
	private->extensible.extnodename = OptionCPrivateNodeName;
	private->collectionId = (int) collectionId;
	private->indexId = indexId;
	private->nfields = quals->nfields;
	for (int i = 0; i < quals->nfields; i++)
	{
		OptionCPlanField *field = &quals->fields[i];
		private->columns[i] = field->column;
		private->kinds[i] = (int) field->valueKind;
		private->values[i] = pstrdup(field->valueText);
	}

	return list_make1(private);
}

void
TryAddOptionCIndexScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
						   uint64 collectionId)
{
	if (root->parse->commandType != CMD_SELECT || root->rowMarks != NIL || root->parent_root != NULL)
		return;

	OptionCExtractedQuals quals = { 0 };

	ListCell *cell;
	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);
		(void) OptionCExtractEqualityQual(rinfo, &quals);
	}

	if (quals.nfields <= 0)
	{
		return;
	}

	int32 indexId = 0;
	if (!OptionCLookupIndex(collectionId, &quals, &indexId))
	{
		return;
	}

	CustomPath *customPath = makeNode(CustomPath);
	RegisterOptionCPrivateNode();

	customPath->methods = &OptionCPathMethods;
	customPath->custom_paths = NIL;
	customPath->custom_private = OptionCBuildPrivate(collectionId, indexId, &quals);

	Path *path = &customPath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;
	path->param_info = NULL;
	path->parallel_safe = false;
	path->parallel_aware = false;
	path->rows = 100;
	path->startup_cost = 0.0;
	path->total_cost = 1.0;
	path->pathkeys = NIL;

#if (PG_VERSION_NUM >= 150000)
	customPath->flags = CUSTOMPATH_SUPPORT_PROJECTION;
#endif

	add_path(rel, (Path *) customPath);
}

static Plan *
OptionCPlanCustomPath(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *bestPath,
					  List *tlist, List *clauses, List *customPlans)
{
	CustomScan *cscan = makeNode(CustomScan);
	cscan->methods = &OptionCScanMethods;
	cscan->scan.scanrelid = rel->relid;
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->custom_private = bestPath->custom_private;
	cscan->custom_plans = NIL;
	cscan->custom_scan_tlist = NIL;
	return (Plan *) cscan;
}

static Node *
OptionCCreateCustomScanState(CustomScan *cscan)
{
	OptionCScanState *state = (OptionCScanState *) newNode(sizeof(OptionCScanState),
											 T_CustomScanState);
	state->custom_scanstate.methods = &OptionCExecMethods;
	return (Node *) &state->custom_scanstate;
}

static void
OptionCAppendConditions(StringInfo sql, OptionCPrivate *private)
{
	for (int i = 0; i < private->nfields; i++)
	{
		int col = private->columns[i];
		int kind = private->kinds[i];
		char *value = private->values[i];

		if (kind == OPTION_C_VALUE_NUMERIC)
		{
			appendStringInfo(sql, " AND e.f%d_n = %s::double precision", col, value);
		}
		else if (kind == OPTION_C_VALUE_TEXT)
		{
			char *quoted = quote_literal_cstr(value);
			appendStringInfo(sql, " AND e.f%d_t = %s", col, quoted);
			pfree(quoted);
		}
	}
}

static void
OptionCBeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	OptionCScanState *state = (OptionCScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	OptionCPrivate *private = (OptionCPrivate *) linitial(cscan->custom_private);
	int collectionId = private->collectionId;
	int indexId = private->indexId;

	StringInfoData sql;
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT d.shard_key_value, d.object_id, d.document, d.creation_time "
					 "FROM %s.ic_%d_%d e "
					 "JOIN %s.documents_%d d "
					 "ON d.object_id OPERATOR(%s.=) e.document_id "
					 "WHERE d.shard_key_value = %d",
					 ApiDataSchemaName, collectionId, indexId,
					 ApiDataSchemaName, collectionId,
					 CoreSchemaName, collectionId);
	OptionCAppendConditions(&sql, private);

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("option_c scan: could not connect to SPI")));
	}

	int spiStatus = SPI_execute(sql.data, true, 0);
	pfree(sql.data);
	if (spiStatus != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("option_c scan: SPI query failed with status %d", spiStatus)));
	}

	/*
	 * Copy result rows into executor memory BEFORE calling SPI_finish().
	 *
	 * When the custom scan is invoked from within an outer SPI query
	 * (e.g. the pre-SELECT in DeleteAllMatchingDocuments), SPI_connect()
	 * shifts _SPI_current to a new inner level.  If we leave SPI connected
	 * during ExecutorRun, the outer query's DEST callback writes its rows
	 * into our inner tuptable instead of the caller's, yielding zero rows
	 * for the outer query.  Finishing SPI here restores _SPI_current to
	 * the outer level before ExecutorRun begins.
	 */
	uint64 nrows = SPI_processed;
	HeapTuple *rowsCopy = NULL;
	TupleDesc tupdescCopy = NULL;

	if (nrows > 0)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(estate->es_query_cxt);
		rowsCopy    = (HeapTuple *) palloc(nrows * sizeof(HeapTuple));
		tupdescCopy = CreateTupleDescCopy(SPI_tuptable->tupdesc);
		for (uint64 i = 0; i < nrows; i++)
			rowsCopy[i] = heap_copytuple(SPI_tuptable->vals[i]);
		MemoryContextSwitchTo(oldCtx);
	}

	SPI_finish();
	state->spiConnected = false;

	state->rows      = rowsCopy;
	state->tupdesc   = tupdescCopy;
	state->processed = nrows;
	state->nextRow   = 0;
}

static TupleTableSlot *
OptionCExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss, (ExecScanAccessMtd) OptionCNext,
				(ExecScanRecheckMtd) OptionCRecheck);
}

static TupleTableSlot *
OptionCNext(CustomScanState *node)
{
	OptionCScanState *state = (OptionCScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (state->nextRow >= state->processed)
	{
		return ExecClearTuple(slot);
	}

	HeapTuple tuple = state->rows[state->nextRow++];
	TupleDesc spiDesc = state->tupdesc;
	TupleDesc slotDesc = slot->tts_tupleDescriptor;

	ExecClearTuple(slot);
	for (int i = 0; i < slotDesc->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}

	for (int i = 0; i < slotDesc->natts && i < spiDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(slotDesc, i);
		bool isNull = false;
		Datum value = SPI_getbinval(tuple, spiDesc, i + 1, &isNull);

		if (!isNull)
		{
			value = datumCopy(value, attr->attbyval, attr->attlen);
		}
		slot->tts_values[i] = value;
		slot->tts_isnull[i] = isNull;
	}
	ExecStoreVirtualTuple(slot);
	return slot;
}

static bool
OptionCRecheck(ScanState *state, TupleTableSlot *slot)
{
	return true;
}

static void
OptionCEndCustomScan(CustomScanState *node)
{
	OptionCScanState *state = (OptionCScanState *) node;
	if (state->spiConnected)
	{
		SPI_finish();
		state->spiConnected = false;
	}
}

static void
OptionCReScanCustomScan(CustomScanState *node)
{
	OptionCScanState *state = (OptionCScanState *) node;
	state->nextRow = 0;
}

static void
OptionCExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	OptionCPrivate *private = (OptionCPrivate *) linitial(cscan->custom_private);

	ExplainPropertyInteger("Option C Collection", NULL, private->collectionId, es);
	ExplainPropertyInteger("Option C Index", NULL, private->indexId, es);
	ExplainPropertyText("Option C Access", "ic_ side table", es);
}
