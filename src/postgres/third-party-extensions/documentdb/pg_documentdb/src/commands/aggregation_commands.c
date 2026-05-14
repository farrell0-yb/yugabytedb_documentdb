/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/commands/aggregation_cursors.c
 *
 * Implementation of the cursor based operations for aggregation/find queries.
 * This wraps around the query
 *
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <funcapi.h>
#include <utils/varlena.h>
#include <access/xact.h>
#include <storage/proc.h>
#include <stdlib.h>

#include <metadata/metadata_cache.h>
#include <utils/documentdb_errors.h>
#include "utils/version_utils.h"
#include <io/bson_core.h>
#include <commands/cursor_private.h>
#include "commands/parse_error.h"
#include <aggregation/bson_aggregation_pipeline.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/array.h>


extern bool EnableNowSystemVariable;

/* --------------------------------------------------------- */
/* Data types */
/* --------------------------------------------------------- */


static const int64_t CursorAcceptableBitsMask = 0x1FFFFFFFFFFFFF;

/*
 * Enum for the type of cursor for this query.
 */
typedef enum CursorKind
{
	/*
	 * The cursor is a streaming cursor.
	 */
	CursorKind_Streaming = 1,

	/*
	 * The cursor is a persisted cursor.
	 */
	CursorKind_Persisted = 2,

	/*
	 * The cursor is a tailable cursor.
	 */
	CursorKind_Tailable = 3
} CursorKind;


/*
 * The type of query command provided
 */
typedef enum QueryKind
{
	/*
	 * The user query is a 'find' query.
	 */
	QueryKind_Find = 1,

	/*
	 * The user query is a 'aggregate' query.
	 */
	QueryKind_Aggregate = 2,

	/*
	 * The user query is a 'listCollections' query.
	 */
	QueryKind_ListCollections = 3,

	/*
	 * The user query is a 'listIndexes' query.
	 */
	QueryKind_ListIndexes = 4,
} QueryKind;


/*
 * Cursor related info for the subsequent pages of a find/aggregate request (getMore)
 */
typedef struct
{
	/*
	 * Whether the first request was streamable or persisted
	 */
	CursorKind cursorKind;

	/*
	 * CursorId associated with this query.
	 */
	int64_t cursorId;

	/*
	 * The persisted cursor name in postgres.
	 */
	const char *cursorName;

	/*
	 * The query spec for a streamable cursor.
	 */
	pgbson *querySpec;

	/*
	 * The original query's query kind (find/aggregate)
	 */
	QueryKind queryKind;

	/*
	 * The current page's cursor info.
	 */
	QueryData queryData;
} QueryGetMoreInfo;

/* --------------------------------------------------------- */
/* Forward declaration */
/* --------------------------------------------------------- */

static void ParseGetMoreSpec(text *databaseName, pgbson *getMoreSpec, pgbson *cursorSpec,
							 QueryGetMoreInfo *getMoreInfo);

static pgbson * BuildStreamingContinuationDocument(HTAB *cursorMap, pgbson *querySpec,
												   int64_t cursorId, QueryKind queryKind,
												   TimeSystemVariables *
												   timeSystemVariables,
												   int numIterations, bool
												   isTailableCursor);

static pgbson * BuildPersistedContinuationDocument(const char *cursorName, int64_t
												   cursorId, QueryKind queryKind,
												   TimeSystemVariables *
												   timeSystemVariables,
												   int numIterations);

static Datum HandleFirstPageRequest(PG_FUNCTION_ARGS,
									text *database, pgbson *querySpec, int64_t cursorId,
									QueryData *cursorState,
									QueryKind queryKind, Query *query);

static int64_t GenerateCursorId(int64_t inputValue);

/* Generates a base QueryData used for the first page */
inline static QueryData
GenerateFirstPageQueryData(void)
{
	QueryData queryData = { 0 };
	queryData.batchSize = 101;
	return queryData;
}


/* --------------------------------------------------------- */
/* Top level exports */
/* --------------------------------------------------------- */

PG_FUNCTION_INFO_V1(command_aggregate_cursor_first_page);
PG_FUNCTION_INFO_V1(command_find_cursor_first_page);
PG_FUNCTION_INFO_V1(command_count_query);
PG_FUNCTION_INFO_V1(command_distinct_query);
PG_FUNCTION_INFO_V1(command_cursor_get_more);
PG_FUNCTION_INFO_V1(command_list_collections_cursor_first_page);
PG_FUNCTION_INFO_V1(command_list_indexes_cursor_first_page);

/*
 * Parses an aggregate spec and creates a query, executes it and returns the first page
 * along with the cursor information associated with the aggregate query.
 */
Datum
command_aggregate_cursor_first_page(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *aggregationSpec = PG_GETARG_PGBSON(1);
	int64_t cursorId = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT64(2);

	bool generateCursorParams = true;
	bool setStatementTimeout = true;
	QueryData queryData = GenerateFirstPageQueryData();
	Query *query = GenerateAggregationQuery(database, aggregationSpec, &queryData,
											generateCursorParams, setStatementTimeout);

	Datum response = HandleFirstPageRequest(
		fcinfo, DatumGetTextP(database), aggregationSpec, cursorId, &queryData,
		QueryKind_Aggregate, query);

	PG_RETURN_DATUM(response);
}


/* --------------------------------------------------------- */
/* Option C read-path helpers */
/* --------------------------------------------------------- */

/*
 * Captures a parsed Option C filter: equality on 1–4 fields (compound) or a
 * range on a single field using $gt/$gte/$lt/$lte.
 */
#define OPTION_C_MAX_FILTER_FIELDS 4

typedef struct OptionCFilterInfo
{
	char   *collectionName;
	bool	isEquality;     /* true = equality (1..4 fields); false = single-field range */
	int		nFilterFields;  /* 1..OPTION_C_MAX_FILTER_FIELDS for equality; 1 for range */
	/* Per-field equality values */
	char   *fieldName[OPTION_C_MAX_FILTER_FIELDS];
	bool	isNumeric[OPTION_C_MAX_FILTER_FIELDS];
	double	numVal[OPTION_C_MAX_FILTER_FIELDS];
	char   *strVal[OPTION_C_MAX_FILTER_FIELDS];
	/* Range bounds (isEquality = false; field is fieldName[0]; isNumeric[0] set) */
	bool	hasLower;
	bool	lowerInclusive;
	double	numLowerValue;
	char   *lowerValue;
	bool	hasUpper;
	bool	upperInclusive;
	double	numUpperValue;
	char   *upperValue;
	/* Sort order (from find spec "sort"); hasSort=false -> no ORDER BY */
	bool	hasSort;
	int		nSortFields;
	char   *sortFieldName[OPTION_C_MAX_FILTER_FIELDS];
	bool	sortDesc[OPTION_C_MAX_FILTER_FIELDS];
} OptionCFilterInfo;

/*
 * ExtractOptionCFilter parses a find spec and returns true when the filter
 * contains 1–4 equality fields with supported scalar values, or exactly one
 * field with a range sub-document using $gt/$gte/$lt/$lte.
 * On success all relevant fields of *out are populated with palloc'd copies.
 */

static int
OptionCTypeFamily(int typeCode)
{
	if (typeCode == BSON_TYPE_DOUBLE || typeCode == BSON_TYPE_INT32 ||
		typeCode == BSON_TYPE_INT64 || typeCode == BSON_TYPE_DECIMAL128)
		return 1;
	if (typeCode == BSON_TYPE_UTF8)
		return 2;
	if (typeCode == BSON_TYPE_OID)
		return 3;
	if (typeCode == BSON_TYPE_BOOL)
		return 4;
	if (typeCode == BSON_TYPE_DATE_TIME)
		return 5;
	return 0;
}

static bool
OptionCBsonIterToFilterValue(bson_iter_t *iter, bool *isNumeric, double *numVal,
							 char **strVal, int *typeCode)
{
	bson_type_t t = bson_iter_type(iter);
	*isNumeric = false;
	*numVal = 0.0;
	*strVal = NULL;
	*typeCode = (int) t;

	if (t == BSON_TYPE_UTF8)
	{
		uint32_t len;
		const char *val = bson_iter_utf8(iter, &len);
		*strVal = pnstrdup(val, len);
		return true;
	}
	if (t == BSON_TYPE_OID)
	{
		char oidStr[25];
		bson_oid_to_string(bson_iter_oid(iter), oidStr);
		*strVal = pstrdup(oidStr);
		return true;
	}
	if (t == BSON_TYPE_INT32)
	{
		*isNumeric = true;
		*numVal = (double) bson_iter_int32(iter);
		return true;
	}
	if (t == BSON_TYPE_INT64)
	{
		*isNumeric = true;
		*numVal = (double) bson_iter_int64(iter);
		return true;
	}
	if (t == BSON_TYPE_DOUBLE)
	{
		*isNumeric = true;
		*numVal = bson_iter_double(iter);
		return true;
	}
	if (t == BSON_TYPE_BOOL)
	{
		*isNumeric = true;
		*numVal = bson_iter_bool(iter) ? 1.0 : 0.0;
		return true;
	}
	if (t == BSON_TYPE_DATE_TIME)
	{
		*isNumeric = true;
		*numVal = (double) bson_iter_date_time(iter);
		return true;
	}
	if (t == BSON_TYPE_DECIMAL128)
	{
		bson_decimal128_t dec;
		char decStr[BSON_DECIMAL128_STRING];
		bson_iter_decimal128(iter, &dec);
		bson_decimal128_to_string(&dec, decStr);
		*isNumeric = true;
		*numVal = strtod(decStr, NULL);
		return true;
	}

	return false;
}

/*
 * ExtractOptionCFilter parses a find spec and returns true when the filter
 * contains 1–4 equality fields with supported scalar values, or exactly one
 * field with a range sub-document using $gt/$gte/$lt/$lte.
 */
static bool
ExtractOptionCFilter(pgbson *findSpec, OptionCFilterInfo *out)
{
	bson_iter_t iter;
	PgbsonInitIterator(findSpec, &iter);

	const char *collectionName = NULL;
	bool filterSeen = false;
	bool filterMatched = false;

	memset(out, 0, sizeof(*out));

	while (bson_iter_next(&iter))
	{
		const char *key = bson_iter_key(&iter);

		if (strcmp(key, "find") == 0)
		{
			if (bson_iter_type(&iter) != BSON_TYPE_UTF8)
				return false;
			uint32_t len;
			const char *val = bson_iter_utf8(&iter, &len);
			collectionName = pnstrdup(val, len);
		}
		else if (strcmp(key, "filter") == 0)
		{
			filterSeen = true;
			if (bson_iter_type(&iter) != BSON_TYPE_DOCUMENT)
				return false;

			bson_iter_t filterIter;
			if (!bson_iter_recurse(&iter, &filterIter))
				return false;

			int nkeys = 0;
			bool hasRange = false;
			while (bson_iter_next(&filterIter))
			{
				nkeys++;
				if (nkeys > OPTION_C_MAX_FILTER_FIELDS)
					return false;

				const char *filterKey = bson_iter_key(&filterIter);
				if (filterKey[0] == '$')
					return false;

				int fieldIdx = nkeys - 1;
				out->fieldName[fieldIdx] = pstrdup(filterKey);

				if (bson_iter_type(&filterIter) != BSON_TYPE_DOCUMENT)
				{
					/* Scalar equality value */
					bool isNumeric;
					double numericVal;
					char *strVal;
					int typeCode;
					if (!OptionCBsonIterToFilterValue(&filterIter, &isNumeric, &numericVal,
												 &strVal, &typeCode))
						return false;
					out->isNumeric[fieldIdx] = isNumeric;
					out->numVal[fieldIdx] = numericVal;
					out->strVal[fieldIdx] = strVal;
					filterMatched = true;
				}
				else
				{
					/* Range sub-document — only allowed for single-field filters */
					if (nkeys > 1)
						return false;

					bson_iter_t rangeIter;
					if (!bson_iter_recurse(&filterIter, &rangeIter))
						return false;

					int nops = 0;
					int rangeFamily = 0;
					bool rangeIsNumeric = false;
					while (bson_iter_next(&rangeIter))
					{
						nops++;
						if (nops > 2)
							return false;

						const char *op = bson_iter_key(&rangeIter);
						bool isNumeric;
						double numericVal;
						char *strVal;
						int typeCode;
						if (!OptionCBsonIterToFilterValue(&rangeIter, &isNumeric, &numericVal,
													 &strVal, &typeCode))
							return false;

						int family = OptionCTypeFamily(typeCode);
						if (family == 0 || typeCode == BSON_TYPE_BOOL || typeCode == BSON_TYPE_OID)
							return false;
						if (rangeFamily != 0 && rangeFamily != family)
							return false;
						rangeFamily = family;
						rangeIsNumeric = isNumeric;

						if (strcmp(op, "$gt") == 0)
						{
							if (out->hasLower) return false;
							out->hasLower = true;
							out->lowerInclusive = false;
							if (isNumeric) out->numLowerValue = numericVal;
							else out->lowerValue = strVal;
						}
						else if (strcmp(op, "$gte") == 0)
						{
							if (out->hasLower) return false;
							out->hasLower = true;
							out->lowerInclusive = true;
							if (isNumeric) out->numLowerValue = numericVal;
							else out->lowerValue = strVal;
						}
						else if (strcmp(op, "$lt") == 0)
						{
							if (out->hasUpper) return false;
							out->hasUpper = true;
							out->upperInclusive = false;
							if (isNumeric) out->numUpperValue = numericVal;
							else out->upperValue = strVal;
						}
						else if (strcmp(op, "$lte") == 0)
						{
							if (out->hasUpper) return false;
							out->hasUpper = true;
							out->upperInclusive = true;
							if (isNumeric) out->numUpperValue = numericVal;
							else out->upperValue = strVal;
						}
						else
						{
							return false;
						}
					}

					if (!out->hasLower && !out->hasUpper)
						return false;

					out->isNumeric[0] = rangeIsNumeric;
					hasRange = true;
					filterMatched = true;
				}
			}

			if (nkeys == 0)
				return false;
			if (hasRange && nkeys > 1)
				return false;

			out->isEquality = !hasRange;
			out->nFilterFields = nkeys;
		}
		else if (strcmp(key, "sort") == 0)
		{
			/* Parse sort spec: {field: 1 or -1}.  Accept up to
			 * OPTION_C_MAX_FILTER_FIELDS fields.  Any non-integer or
			 * non-unit value silently leaves hasSort=false so the caller
			 * falls back to the unordered EXISTS path. */
			if (bson_iter_type(&iter) == BSON_TYPE_DOCUMENT)
			{
				bson_iter_t sortIter;
				if (bson_iter_recurse(&iter, &sortIter))
				{
					int nsort = 0;
					bool sortOk = true;
					while (bson_iter_next(&sortIter) && sortOk)
					{
						if (nsort >= OPTION_C_MAX_FILTER_FIELDS)
							{ sortOk = false; break; }
						const char *sk = bson_iter_key(&sortIter);
						int32_t dir = 0;
						if (bson_iter_type(&sortIter) == BSON_TYPE_INT32)
							dir = bson_iter_int32(&sortIter);
						else if (bson_iter_type(&sortIter) == BSON_TYPE_INT64)
							dir = (int32_t) bson_iter_int64(&sortIter);
						else if (bson_iter_type(&sortIter) == BSON_TYPE_DOUBLE)
							dir = (int32_t) bson_iter_double(&sortIter);
						else
							{ sortOk = false; break; }
						if (dir != 1 && dir != -1)
							{ sortOk = false; break; }
						out->sortFieldName[nsort] = pstrdup(sk);
						out->sortDesc[nsort] = (dir == -1);
						nsort++;
					}
					if (sortOk && nsort > 0)
					{
						out->hasSort = true;
						out->nSortFields = nsort;
					}
				}
			}
		}
	}

	if (!filterSeen || !filterMatched || collectionName == NULL)
		return false;

	out->collectionName = (char *) collectionName;
	return true;
}

/*
 * LookupOptionCIndex looks up collection_id and index_id for an Option C index
 * whose field_paths exactly covers filterFieldNames[0..nFilterFields-1].
 * On success, outColForFilter[j] is the zero-based column position in the
 * ic_ table for filterFieldNames[j].
 * Must be called outside an existing SPI connection.
 */
static bool
LookupOptionCIndex(const char *databaseName,
				   const char *collectionName,
				   int nFilterFields,
				   char **filterFieldNames,
				   uint64 *outCollectionId,
				   int32 *outIndexId,
				   int *outColForFilter)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("option_c_lookup: could not connect to SPI")));

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT c.collection_id, ci.index_id, ci.field_paths "
					 "FROM %s.collections c "
					 "JOIN %s.option_c_indexes ci "
					 "  ON ci.collection_id = c.collection_id "
					 "WHERE c.database_name = $1 "
					 "  AND c.collection_name = $2 "
					 "  AND ci.field_paths @> ARRAY[",
					 ApiCatalogSchemaName, ApiCatalogSchemaName);
	for (int i = 0; i < nFilterFields; i++)
	{
		if (i > 0) appendStringInfoChar(&query, ',');
		appendStringInfo(&query, "$%d", i + 3);
	}
	appendStringInfo(&query,
					 "]::text[] "
					 "  AND cardinality(ci.field_paths) = %d "
					 "  AND ci.is_valid "
					 "LIMIT 1",
					 nFilterFields);

	int nArgs = 2 + nFilterFields;
	Oid *argTypes = (Oid *) palloc(nArgs * sizeof(Oid));
	Datum *argValues = (Datum *) palloc(nArgs * sizeof(Datum));
	char *nulls = (char *) palloc(nArgs * sizeof(char));

	argTypes[0] = TEXTOID;
	argValues[0] = CStringGetTextDatum(databaseName);
	nulls[0] = ' ';
	argTypes[1] = TEXTOID;
	argValues[1] = CStringGetTextDatum(collectionName);
	nulls[1] = ' ';
	for (int i = 0; i < nFilterFields; i++)
	{
		argTypes[2 + i] = TEXTOID;
		argValues[2 + i] = CStringGetTextDatum(filterFieldNames[i]);
		nulls[2 + i] = ' ';
	}

	int spiStatus = SPI_execute_with_args(query.data, nArgs, argTypes, argValues,
										  nulls, true, 1);
	pfree(query.data);

	if (spiStatus != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return false;
	}

	bool isNull;
	Datum collIdDatum = SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc, 1, &isNull);
	if (isNull) { SPI_finish(); return false; }
	*outCollectionId = (uint64) DatumGetInt64(collIdDatum);

	Datum indexIdDatum = SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc, 2, &isNull);
	if (isNull) { SPI_finish(); return false; }
	*outIndexId = DatumGetInt32(indexIdDatum);

	Datum fpDatum = SPI_getbinval(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc, 3, &isNull);
	if (isNull) { SPI_finish(); return false; }

	ArrayType *fp = DatumGetArrayTypeP(fpDatum);
	Datum *fpElems;
	bool *fpNulls;
	int fpNElems;
	deconstruct_array(fp, TEXTOID, -1, false, TYPALIGN_INT,
					  &fpElems, &fpNulls, &fpNElems);

	for (int j = 0; j < nFilterFields; j++)
		outColForFilter[j] = -1;

	for (int pos = 0; pos < fpNElems; pos++)
	{
		if (fpNulls[pos]) continue;
		const char *fpName = TextDatumGetCString(fpElems[pos]);
		for (int j = 0; j < nFilterFields; j++)
		{
			if (strcmp(filterFieldNames[j], fpName) == 0)
			{
				outColForFilter[j] = pos;
				break;
			}
		}
	}

	/* Verify every filter field was mapped to a column */
	for (int j = 0; j < nFilterFields; j++)
	{
		if (outColForFilter[j] < 0)
		{
			SPI_finish();
			return false;
		}
	}

	SPI_finish();
	return true;
}

/*
 * ExecuteOptionCFindFirstPage probes an ic_ index table using an EXISTS
 * semi-join and returns a fully-formed cursor-page datum.
 */
static Datum
ExecuteOptionCFindFirstPage(PG_FUNCTION_ARGS,
							 const char *namespaceName,
							 int64_t cursorId,
							 uint64 collectionId,
							 int32 indexId,
							 OptionCFilterInfo *filter,
							 int *colForFilter)
{
	MemoryContext callerCtx = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("option_c_find: could not connect to SPI")));

	/* Build EXISTS conditions and bind params */
	StringInfoData conds;
	initStringInfo(&conds);

	int maxParams = OPTION_C_MAX_FILTER_FIELDS * 2;
	Oid *argTypes = (Oid *) palloc(maxParams * sizeof(Oid));
	Datum *argValues = (Datum *) palloc(maxParams * sizeof(Datum));
	char *nulls = (char *) palloc(maxParams * sizeof(char));
	int nParams = 0;

	if (filter->isEquality)
	{
		for (int j = 0; j < filter->nFilterFields; j++)
		{
			int colPos = colForFilter[j];
			if (filter->isNumeric[j])
			{
				appendStringInfo(&conds, " AND e.f%d_n = $%d", colPos, nParams + 1);
				argTypes[nParams] = FLOAT8OID;
				argValues[nParams] = Float8GetDatum(filter->numVal[j]);
			}
			else
			{
				appendStringInfo(&conds, " AND e.f%d_t = $%d", colPos, nParams + 1);
				argTypes[nParams] = TEXTOID;
				argValues[nParams] = CStringGetTextDatum(filter->strVal[j]);
			}
			nulls[nParams] = ' ';
			nParams++;
		}
	}
	else
	{
		/* Range on field 0 */
		int colPos = colForFilter[0];
		if (filter->isNumeric[0])
		{
			if (filter->hasLower)
			{
				appendStringInfo(&conds, " AND e.f%d_n %s $%d",
								 colPos, filter->lowerInclusive ? ">=" : ">", nParams + 1);
				argTypes[nParams] = FLOAT8OID;
				argValues[nParams] = Float8GetDatum(filter->numLowerValue);
				nulls[nParams++] = ' ';
			}
			if (filter->hasUpper)
			{
				appendStringInfo(&conds, " AND e.f%d_n %s $%d",
								 colPos, filter->upperInclusive ? "<=" : "<", nParams + 1);
				argTypes[nParams] = FLOAT8OID;
				argValues[nParams] = Float8GetDatum(filter->numUpperValue);
				nulls[nParams++] = ' ';
			}
		}
		else
		{
			if (filter->hasLower)
			{
				appendStringInfo(&conds, " AND e.f%d_t %s $%d",
								 colPos, filter->lowerInclusive ? ">=" : ">", nParams + 1);
				argTypes[nParams] = TEXTOID;
				argValues[nParams] = CStringGetTextDatum(filter->lowerValue);
				nulls[nParams++] = ' ';
			}
			if (filter->hasUpper)
			{
				appendStringInfo(&conds, " AND e.f%d_t %s $%d",
								 colPos, filter->upperInclusive ? "<=" : "<", nParams + 1);
				argTypes[nParams] = TEXTOID;
				argValues[nParams] = CStringGetTextDatum(filter->upperValue);
				nulls[nParams++] = ' ';
			}
		}
	}

	/* Map sort fields to ic_ column positions using colForFilter[] */
	StringInfoData orderBy;
	initStringInfo(&orderBy);
	bool sortResolved = false;
	if (filter->hasSort)
	{
		bool allMapped = true;
		for (int k = 0; k < filter->nSortFields && allMapped; k++)
		{
			int icCol = -1;
			for (int j = 0; j < filter->nFilterFields; j++)
			{
				if (strcmp(filter->sortFieldName[k], filter->fieldName[j]) == 0)
					{ icCol = colForFilter[j]; break; }
			}
			if (icCol < 0) { allMapped = false; break; }
			const char *dir = filter->sortDesc[k] ? "DESC" : "ASC";
			if (orderBy.len > 0) appendStringInfoString(&orderBy, ", ");
			appendStringInfo(&orderBy,
							 "e.f%d_n %s NULLS LAST, e.f%d_t %s NULLS LAST",
							 icCol, dir, icCol, dir);
		}
		sortResolved = allMapped && (orderBy.len > 0);
	}

	StringInfoData query;
	initStringInfo(&query);
	if (sortResolved)
	{
		/* JOIN lets the planner use the ic_ LSM index for ORDER BY */
		appendStringInfo(&query,
						 "SELECT d.document "
						 "FROM %s.ic_%lu_%d e "
						 "JOIN %s.documents_%lu d "
						 "  ON e.document_id OPERATOR(%s.=) d.object_id"
						 " WHERE true%s"
						 " ORDER BY %s",
						 ApiDataSchemaName, (unsigned long) collectionId, indexId,
						 ApiDataSchemaName, (unsigned long) collectionId,
						 CoreSchemaName,
						 conds.data,
						 orderBy.data);
	}
	else
	{
		appendStringInfo(&query,
						 "SELECT d.document "
						 "FROM %s.documents_%lu d "
						 "WHERE EXISTS ("
						 "  SELECT 1 FROM %s.ic_%lu_%d e "
						 "  WHERE e.document_id OPERATOR(%s.=) d.object_id"
						 "%s"
						 ")",
						 ApiDataSchemaName, (unsigned long) collectionId,
						 ApiDataSchemaName, (unsigned long) collectionId, indexId,
						 CoreSchemaName,
						 conds.data);
	}
	pfree(conds.data);
	pfree(orderBy.data);

	int spiStatus = SPI_execute_with_args(query.data, nParams, argTypes, argValues,
										  nulls, true, 0);
	pfree(query.data);

	if (spiStatus != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR, (errcode(ERRCODE_DOCUMENTDB_INTERNALERROR),
						errmsg("Option C index probe failed")));
	}

	uint64 nrows = SPI_processed;
	pgbson **docs = NULL;

	if (nrows > 0)
	{
		MemoryContext spiCtx = MemoryContextSwitchTo(callerCtx);
		docs = (pgbson **) palloc0(nrows * sizeof(pgbson *));
		MemoryContextSwitchTo(spiCtx);

		for (uint64 i = 0; i < nrows; i++)
		{
			bool isNull;
			Datum docDatum = SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc, 1, &isNull);
			if (!isNull)
			{
				pgbson *spiDoc = (pgbson *) PG_DETOAST_DATUM(docDatum);
				uint32 docSize = VARSIZE_ANY(spiDoc);

				MemoryContext spiCtx2 = MemoryContextSwitchTo(callerCtx);
				pgbson *callerDoc = (pgbson *) palloc(docSize);
				memcpy(callerDoc, spiDoc, docSize);
				MemoryContextSwitchTo(spiCtx2);
				docs[i] = callerDoc;
			}
		}
	}

	SPI_finish();

	pgbson_writer topWriter;
	pgbson_writer cursorDoc;
	pgbson_array_writer arrayWriter;
	uint32_t accumulatedSize = 5;

	SetupCursorPagePreamble(&topWriter, &cursorDoc, &arrayWriter,
							 namespaceName, true, &accumulatedSize);

	for (uint64 i = 0; i < nrows; i++)
	{
		if (docs[i] != NULL)
			PgbsonArrayWriterWriteDocument(&arrayWriter, docs[i]);
	}

	return PostProcessCursorPage(fcinfo, &cursorDoc, &arrayWriter, &topWriter,
								  0, NULL, false, NULL);
}

/*
 * Parses an find spec and creates a query, executes it and returns the first page
 * along with the cursor information associated with the find query.
 */
Datum
command_find_cursor_first_page(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *findSpec = PG_GETARG_PGBSON(1);
	int64_t cursorId = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT64(2);

	/* Option C read-path intercept */
	{
		OptionCFilterInfo filter;
		if (ExtractOptionCFilter(findSpec, &filter))
		{
			const char *databaseName = text_to_cstring(DatumGetTextPP(database));
			uint64 collectionId;
			int32 indexId;
			int colForFilter[OPTION_C_MAX_FILTER_FIELDS];
			if (LookupOptionCIndex(databaseName, filter.collectionName,
								   filter.nFilterFields, filter.fieldName,
								   &collectionId, &indexId, colForFilter))
			{
				StringInfoData ns;
				initStringInfo(&ns);
				appendStringInfo(&ns, "%s.%s", databaseName, filter.collectionName);
				PG_RETURN_DATUM(ExecuteOptionCFindFirstPage(fcinfo, ns.data, cursorId,
														   collectionId, indexId,
														   &filter, colForFilter));
			}
		}
	}

	/* Parse the find spec for the purposes of query execution */
	QueryData queryData = GenerateFirstPageQueryData();
	bool generateCursorParams = true;
	bool setStatementTimeout = true;
	Query *query = GenerateFindQuery(database, findSpec, &queryData,
									 generateCursorParams,
									 setStatementTimeout);

	Datum response = HandleFirstPageRequest(
		fcinfo, DatumGetTextPP(database), findSpec, cursorId, &queryData,
		QueryKind_Find, query);

	PG_RETURN_DATUM(response);
}


/*
 * Parses a listCollections spec and creates a query, executes it and returns the first page
 * along with the cursor information associated with the listCollections query.
 */
Datum
command_list_collections_cursor_first_page(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *listCollectionsSpec = PG_GETARG_PGBSON(1);
	QueryData queryData = GenerateFirstPageQueryData();
	bool generateCursorParams = false;
	bool setStatementTimeout = true;
	Query *query = GenerateListCollectionsQuery(database, listCollectionsSpec, &queryData,
												generateCursorParams,
												setStatementTimeout);

	/* TODO: Remove these restrictions */
	queryData.cursorKind = QueryCursorType_SingleBatch;
	queryData.batchSize = INT_MAX;

	int64_t cursorId = 0;
	Datum response = HandleFirstPageRequest(
		fcinfo, DatumGetTextP(database), listCollectionsSpec, cursorId, &queryData,
		QueryKind_ListCollections, query);

	PG_RETURN_DATUM(response);
}


/*
 * Parses a listIndexes spec and creates a query, executes it and returns the first page
 * along with the cursor information associated with the listIndexes query.
 */
Datum
command_list_indexes_cursor_first_page(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *listIndexesSpec = PG_GETARG_PGBSON(1);
	QueryData queryData = GenerateFirstPageQueryData();
	bool generateCursorParams = false;
	bool setStatementTimeout = true;
	Query *query = GenerateListIndexesQuery(database, listIndexesSpec, &queryData,
											generateCursorParams, setStatementTimeout);

	/* TODO: Remove these restrictions */
	queryData.cursorKind = QueryCursorType_SingleBatch;
	queryData.batchSize = INT_MAX;

	int64_t cursorId = 0;
	Datum response = HandleFirstPageRequest(
		fcinfo, DatumGetTextP(database), listIndexesSpec, cursorId, &queryData,
		QueryKind_ListIndexes, query);

	PG_RETURN_DATUM(response);
}


/*
 * Parses a getMore spec and a continuation cursor spec, extracts the query
 * associated with it executes it and returns the next page
 * along with the cursor information associated with the original query.
 */
Datum
command_cursor_get_more(PG_FUNCTION_ARGS)
{
	text *database = PG_GETARG_TEXT_P(0);
	pgbson *getMoreSpec = PG_GETARG_PGBSON(1);
	pgbson *cursorSpec = PG_GETARG_PGBSON(2);

	QueryGetMoreInfo getMoreInfo = { 0 };
	ParseGetMoreSpec(database, getMoreSpec, cursorSpec, &getMoreInfo);

	pgbson_writer writer;
	pgbson_writer cursorDoc;
	pgbson_array_writer arrayWriter;

	/* min bson size is 5 (see IsPgbsonEmptyDocument) */
	uint32_t accumulatedSize = 5;

	/* Write the preamble for the cursor response */
	bool isFirstPage = false;
	SetupCursorPagePreamble(&writer, &cursorDoc, &arrayWriter,
							getMoreInfo.queryData.namespaceName,
							isFirstPage,
							&accumulatedSize);

	bool queryFullyDrained;
	pgbson *continuationDoc;
	pgbson *postBatchResumeToken = NULL;
	switch (getMoreInfo.cursorKind)
	{
		case CursorKind_Persisted:
		{
			int numIterations = 0;
			queryFullyDrained = DrainPersistedCursor(getMoreInfo.cursorName,
													 getMoreInfo.queryData.batchSize,
													 &numIterations,
													 accumulatedSize, &arrayWriter);
			continuationDoc = queryFullyDrained ? NULL :
							  BuildPersistedContinuationDocument(getMoreInfo.cursorName,
																 getMoreInfo.cursorId,
																 getMoreInfo.queryKind,
																 &getMoreInfo.queryData.
																 timeSystemVariables,
																 numIterations);
			break;
		}

		case CursorKind_Streaming:
		{
			Query *query;
			bool generateCursorParams = true;

			/* Some blank query data to pass to the generation. */
			QueryData queryData = { 0 };
			switch (getMoreInfo.queryKind)
			{
				case QueryKind_Find:
				{
					queryData.timeSystemVariables =
						getMoreInfo.queryData.timeSystemVariables;

					bool setStatementTimeout = false;
					query = GenerateFindQuery(PointerGetDatum(database),
											  getMoreInfo.querySpec, &queryData,
											  generateCursorParams,
											  setStatementTimeout);
					break;
				}

				case QueryKind_Aggregate:
				{
					queryData.timeSystemVariables =
						getMoreInfo.queryData.timeSystemVariables;

					bool setStatementTimeout = false;
					query = GenerateAggregationQuery(PointerGetDatum(database),
													 getMoreInfo.querySpec, &queryData,
													 generateCursorParams,
													 setStatementTimeout);
					break;
				}

				default:
				{
					Assert(false);
					pg_unreachable();
				}
			}

			HTAB *cursorMap = CreateCursorHashSet();
			BuildContinuationMap(cursorSpec, cursorMap);


			int numIterations = 0;
			queryFullyDrained = DrainStreamingQuery(cursorMap, query,
													getMoreInfo.queryData.batchSize,
													&numIterations,
													accumulatedSize, &arrayWriter);
			continuationDoc = queryFullyDrained ? NULL :
							  BuildStreamingContinuationDocument(cursorMap,
																 getMoreInfo.querySpec,
																 getMoreInfo.cursorId,
																 getMoreInfo.queryKind,
																 &getMoreInfo.queryData.
																 timeSystemVariables,
																 numIterations, false);
			hash_destroy(cursorMap);
			break;
		}

		case CursorKind_Tailable:
		{
			Query *query;
			bool generateCursorParams = true;
			QueryData queryData = { 0 };
			queryData.timeSystemVariables = getMoreInfo.queryData.timeSystemVariables;

			bool setStatementTimeout = false;
			query = GenerateAggregationQuery(PointerGetDatum(database),
											 getMoreInfo.querySpec, &queryData,
											 generateCursorParams, setStatementTimeout);
			HTAB *cursorMap = CreateTailableCursorHashSet();
			BuildTailableCursorContinuationMap(cursorSpec, cursorMap);
			int numIterations = 0;
			postBatchResumeToken = DrainTailableQuery(cursorMap, query,
													  getMoreInfo.queryData.batchSize,
													  &numIterations,
													  accumulatedSize, &arrayWriter);
			continuationDoc = BuildStreamingContinuationDocument(cursorMap,
																 getMoreInfo.querySpec,
																 getMoreInfo.cursorId,
																 getMoreInfo.queryKind,
																 &getMoreInfo.queryData.
																 timeSystemVariables,
																 numIterations, true);
			hash_destroy(cursorMap);
			break;
		}

		default:
		{
			Assert(false);
			pg_unreachable();
		}
	}

	bool persistConnection = false;

	Datum responseDatum = PostProcessCursorPage(fcinfo, &cursorDoc, &arrayWriter, &writer,
												getMoreInfo.cursorId, continuationDoc,
												persistConnection, postBatchResumeToken);
	PG_RETURN_DATUM(responseDatum);
}


/*
 * Runs a Distinct query with a given spec against
 * the backend.
 */
Datum
command_distinct_query(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *distinctSpec = PG_GETARG_PGBSON(1);

	bool setStatementTimeout = true;
	Query *query = GenerateDistinctQuery(database, distinctSpec, setStatementTimeout);

	pgbson *response = DrainSingleResultQuery(query);

	if (response == NULL)
	{
		pgbson_writer defaultWriter;
		PgbsonWriterInit(&defaultWriter);
		PgbsonWriterAppendEmptyArray(&defaultWriter, "values", 6);
		PgbsonWriterAppendDouble(&defaultWriter, "ok", 2, 1);
		response = PgbsonWriterGetPgbson(&defaultWriter);
	}

	PG_RETURN_POINTER(response);
}


/*
 * Runs a Count query with a given spec against
 * the backend.
 */
Datum
command_count_query(PG_FUNCTION_ARGS)
{
	Datum database = PG_GETARG_DATUM(0);
	pgbson *countSpec = PG_GETARG_PGBSON(1);

	bool setStatementTimeout = true;
	Query *query = GenerateCountQuery(database, countSpec, setStatementTimeout);

	pgbson *response = DrainSingleResultQuery(query);
	if (response == NULL)
	{
		/* Generate default response */
		pgbson_writer defaultWriter;
		PgbsonWriterInit(&defaultWriter);
		PgbsonWriterAppendInt32(&defaultWriter, "n", 1, 0);
		PgbsonWriterAppendDouble(&defaultWriter, "ok", 2, 1);
		response = PgbsonWriterGetPgbson(&defaultWriter);
	}

	PG_RETURN_POINTER(response);
}


/*
 * Given a pre-built query (for find/aggregate) handles the cursor request
 * and builds a response for the first page.
 */
static Datum
HandleFirstPageRequest(PG_FUNCTION_ARGS,
					   text *database, pgbson *querySpec, int64_t cursorId,
					   QueryData *queryData, QueryKind queryKind, Query *query)
{
	pgbson_writer writer;
	pgbson_writer cursorDoc;
	pgbson_array_writer arrayWriter;

	/* min bson size is 5 (see IsPgbsonEmptyDocument) */
	uint32_t accumulatedSize = 5;

	/* Write the preamble for the cursor response */
	bool isFirstPage = true;
	SetupCursorPagePreamble(&writer, &cursorDoc, &arrayWriter,
							queryData->namespaceName, isFirstPage,
							&accumulatedSize);

	/* now set up the query */
	int32_t numIterations = 0;
	bool queryFullyDrained;
	pgbson *continuationDoc;
	bool persistConnection = false;
	pgbson *postBatchResumeToken = NULL;
	switch (queryData->cursorKind)
	{
		case QueryCursorType_SingleBatch:
		{
			bool isHoldCursor = false;
			bool closeCursor = true;
			CreateAndDrainPersistedQuery("singleBatchCursor", query,
										 queryData->batchSize,
										 &numIterations,
										 accumulatedSize, &arrayWriter,
										 isHoldCursor, closeCursor);
			queryFullyDrained = true;
			continuationDoc = NULL;
			cursorId = 0;
			break;
		}

		case QueryCursorType_Tailable:
		{
			HTAB *tailableCursorMap = CreateTailableCursorHashSet();
			postBatchResumeToken = DrainTailableQuery(tailableCursorMap,
													  query,
													  queryData->batchSize,
													  &numIterations,
													  accumulatedSize,
													  &arrayWriter);
			cursorId = GenerateCursorId(cursorId);
			continuationDoc = BuildStreamingContinuationDocument(tailableCursorMap,
																 querySpec,
																 cursorId, queryKind,
																 &queryData->
																 timeSystemVariables,
																 numIterations, true);
			hash_destroy(tailableCursorMap);
			break;
		}

		case QueryCursorType_Streamable:
		{
			Assert(queryData->cursorStateParamNumber == 1);
			HTAB *cursorMap = CreateCursorHashSet();
			queryFullyDrained = DrainStreamingQuery(cursorMap, query,
													queryData->batchSize,
													&numIterations, accumulatedSize,
													&arrayWriter);

			continuationDoc = NULL;
			if (!queryFullyDrained)
			{
				cursorId = GenerateCursorId(cursorId);
				continuationDoc = BuildStreamingContinuationDocument(cursorMap, querySpec,
																	 cursorId, queryKind,
																	 &queryData->
																	 timeSystemVariables,
																	 numIterations,
																	 false);
			}

			hash_destroy(cursorMap);
			break;
		}

		case QueryCursorType_Persistent:
		{
			/* In order to create the persistent cursor we initialize a cursorId anyway */
			cursorId = GenerateCursorId(cursorId);

			StringInfo cursorStringInfo = makeStringInfo();
			appendStringInfo(cursorStringInfo, "cursor_%ld", cursorId);
			const char *cursorName = cursorStringInfo->data;

			bool isTopLevel = true;
			bool isHoldCursor = !IsInTransactionBlock(isTopLevel);
			persistConnection = isHoldCursor;
			bool closeCursor = false;
			queryFullyDrained = CreateAndDrainPersistedQuery(cursorName, query,
															 queryData->batchSize,
															 &numIterations,
															 accumulatedSize,
															 &arrayWriter,
															 isHoldCursor, closeCursor);
			continuationDoc = queryFullyDrained ? NULL :
							  BuildPersistedContinuationDocument(cursorName, cursorId,
																 queryKind,
																 &queryData->
																 timeSystemVariables,
																 numIterations);
			break;
		}

		case QueryCursorType_PointRead:
		{
			if (queryData->batchSize < 1)
			{
				ereport(ERROR, (errcode(ERRCODE_DOCUMENTDB_INTERNALERROR),
								errmsg(
									"Point read plan should have batch size >= 1, not %d",
									queryData->batchSize),
								errdetail_log(
									"Point read plan should have batch size >= 1, not %d",
									queryData->batchSize)));
			}

			CreateAndDrainPointReadQuery("pointReadCursor", query,
										 &numIterations,
										 accumulatedSize, &arrayWriter);
			queryFullyDrained = true;
			continuationDoc = NULL;
			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_DOCUMENTDB_INTERNALERROR),
							errmsg("Unknown query cursor kind detected - %d",
								   queryData->cursorKind)));
		}
	}

	return PostProcessCursorPage(fcinfo, &cursorDoc, &arrayWriter, &writer, cursorId,
								 continuationDoc, persistConnection,
								 postBatchResumeToken);
}


/*
 * Serializes a cursor document that can be reused by getMore for a streaming query.
 */
static pgbson *
BuildStreamingContinuationDocument(HTAB *cursorMap, pgbson *querySpec, int64_t cursorId,
								   QueryKind queryKind,
								   TimeSystemVariables *timeSystemVariables, int
								   numIterations, bool
								   isTailableCursor)
{
	pgbson_writer writer;
	PgbsonWriterInit(&writer);
	PgbsonWriterAppendInt64(&writer, "qi", 2, cursorId);
	PgbsonWriterAppendBool(&writer, "qp", 2, false);

	PgbsonWriterAppendInt32(&writer, "qk", 2, (int) queryKind);

	/* Add the original query spec so that getMore can reuse it */
	if (isTailableCursor)
	{
		/* For tailable cursor, save the query with "qt" to differentiate from streaming query. */
		PgbsonWriterAppendDocument(&writer, "qt", 2, querySpec);
	}
	else
	{
		/* For streaming cursor, save the query with "qc" key. */
		PgbsonWriterAppendDocument(&writer, "qc", 2, querySpec);
	}

	if (isTailableCursor)
	{
		SerializeTailableContinuationsToWriter(&writer, cursorMap);
	}
	else
	{
		SerializeContinuationsToWriter(&writer, cursorMap);
	}

	/* In the response add the number of iterations (used in tests) */
	PgbsonWriterAppendInt32(&writer, "numIters", 8, numIterations);

	/* Add the time system variables */
	if (EnableNowSystemVariable && IsClusterVersionAtleast(DocDB_V0, 24, 0))
	{
		if (timeSystemVariables != NULL && timeSystemVariables->nowValue.value_type !=
			BSON_TYPE_EOD)
		{
			PgbsonWriterAppendValue(&writer, "sn", 2, &timeSystemVariables->nowValue);
		}
	}

	return PgbsonWriterGetPgbson(&writer);
}


/*
 * Serializes a cursor document that can be reused by getMore for a persitent query.
 */
static pgbson *
BuildPersistedContinuationDocument(const char *cursorName, int64_t cursorId, QueryKind
								   queryKind, TimeSystemVariables *timeSystemVariables,
								   int
								   numIterations)
{
	pgbson_writer writer;
	PgbsonWriterInit(&writer);
	PgbsonWriterAppendInt64(&writer, "qi", 2, cursorId);
	PgbsonWriterAppendBool(&writer, "qp", 2, true);

	/* Add the original query spec so that getMore can reuse it */
	PgbsonWriterAppendInt32(&writer, "qk", 2, (int) queryKind);
	PgbsonWriterAppendUtf8(&writer, "qn", 2, cursorName);

	/* In the response add the number of iterations (used in tests) */
	PgbsonWriterAppendInt32(&writer, "numIters", 8, numIterations);

	/* Add the time system variables */
	if (EnableNowSystemVariable && IsClusterVersionAtleast(DocDB_V0, 24, 0))
	{
		if (timeSystemVariables != NULL && timeSystemVariables->nowValue.value_type !=
			BSON_TYPE_EOD)
		{
			PgbsonWriterAppendValue(&writer, "sn", 2, &timeSystemVariables->nowValue);
		}
	}

	return PgbsonWriterGetPgbson(&writer);
}


/*
 * Parses the serialized cursor spec of the prior iteration. This is the inverse
 * function of BuildStreamingContinuationDocument and BuildPersistedContinuationDocument
 */
static void
ParseCursorInputSpec(pgbson *cursorSpec, QueryGetMoreInfo *getMoreInfo)
{
	bson_iter_t cursorSpecIter;
	PgbsonInitIterator(cursorSpec, &cursorSpecIter);
	while (bson_iter_next(&cursorSpecIter))
	{
		const char *pathKey = bson_iter_key(&cursorSpecIter);
		switch (pathKey[0])
		{
			case 'q':
			{
				switch (pathKey[1])
				{
					/* Query command */
					case 'c':
					{
						/* This is the query command */
						Assert(pathKey[2] == '\0');
						getMoreInfo->querySpec = PgbsonInitFromDocumentBsonValue(
							bson_iter_value(&cursorSpecIter));
						getMoreInfo->cursorKind = CursorKind_Streaming;
						continue;
					}

					/* Query tailable */
					case 't':
					{
						/* This is the query command */
						Assert(pathKey[2] == '\0');
						getMoreInfo->querySpec = PgbsonInitFromDocumentBsonValue(
							bson_iter_value(&cursorSpecIter));
						getMoreInfo->cursorKind = CursorKind_Tailable;
						continue;
					}

					/* Query cursor name */
					case 'n':
					{
						Assert(pathKey[2] == '\0');
						getMoreInfo->cursorName = bson_iter_utf8(&cursorSpecIter, NULL);
						getMoreInfo->cursorKind = CursorKind_Persisted;
						continue;
					}

					/* Query cursor id */
					case 'i':
					{
						Assert(pathKey[2] == '\0');
						getMoreInfo->cursorId = bson_iter_int64(&cursorSpecIter);
						continue;
					}

					/* Query cursor kind */
					case 'k':
					{
						Assert(pathKey[2] == '\0');
						getMoreInfo->queryKind = (QueryKind) bson_iter_int32(
							&cursorSpecIter);
						continue;
					}


					/* Continuation persistence - ignored */
					case 'p':
					{
						continue;
					}
				}

				continue;
			}

			case 's':
			{
				switch (pathKey[1])
				{
					/* $$NOW time system variable (now)*/
					case 'n':
					{
						const bson_value_t *nowDateValue = bson_iter_value(
							&cursorSpecIter);
						getMoreInfo->queryData.timeSystemVariables.nowValue =
							*nowDateValue;
						continue;
					}
				}
				continue;
			}
		}
	}
}


/*
 * Parses the getMore spec and builds the necessary pipeline/query information from a cursor standpoint.
 */
static void
ParseGetMoreSpec(text *databaseName, pgbson *getMoreSpec, pgbson *cursorSpec,
				 QueryGetMoreInfo *getMoreInfo)
{
	/* Default batchSize for getMore */
	getMoreInfo->queryData.batchSize = INT_MAX;

	ParseCursorInputSpec(cursorSpec, getMoreInfo);

	/* Parses the wire protocol getMore */
	bool setStatementTimeout = true;
	int64_t cursorId = ParseGetMore(databaseName, getMoreSpec, &getMoreInfo->queryData,
									setStatementTimeout);
	if (cursorId != getMoreInfo->cursorId)
	{
		ereport(ERROR, (errmsg(
							"CursorID from GetMore does not match from cursor state, getMore: %ld, cursorState %ld",
							cursorId, getMoreInfo->cursorId)));
	}
}


/*
 * Creates a unique cursorId if one isn't provided.
 * We just use virtual x-id since that's going to be unique per query
 * within a node.
 */
static int64_t
GenerateCursorId(int64_t inputValue)
{
	if (inputValue != 0)
	{
		return inputValue;
	}

	/* 2^53-1 masks integer precision of IEEE 754 double precision floating point numbers
	 * Works around issue with certain versions of the NodeJS driver
	 */
	char cursorBuffer[8];

	/* This is the same logic UUID generation uses - we should be good here */
	if (!pg_strong_random(cursorBuffer, 8))
	{
		ereport(ERROR, (errmsg("Unable to generate a unique cursor id")));
	}

	int64_t cursorId = *(int64_t *) cursorBuffer;
	return (cursorId & CursorAcceptableBitsMask);
}
