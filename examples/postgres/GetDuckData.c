#include "duckdbconvert.h"
#include <stdio.h>
#include <stdlib.h>


/* Forward references */
static Datum getMapData(duckdb_logical_type type, duckdb_chunk chunk, idx_t row, idx_t col);
static Datum getListData(duckdb_logical_type type, duckdb_chunk chunk, idx_t row, idx_t col);
static Datum getUnionData(duckdb_logical_type type, duckdb_chunk chunk,idx_t row, idx_t col);
static Datum getStructData(duckdb_logical_type type, duckdb_chunk chunk, idx_t row, idx_t col);
static Datum getBaseData(duckdb_logical_type type, duckdb_chunk chunk, idx_t row, idx_t col);

Datum getDuckData(duckdb_logical_type type, duckdb_chunk chunk, idx_t row, idx_t col)
{
	level++;
	Oid oid;

    /* Get the corresponding postgres type */
	oid = convertDuckType(NULL, type);

	/* Process according to the duckdb type */
	duckdb_type typeid = duckdb_get_type_id(type);
	switch (typeid) {

		case DUCKDB_TYPE_MAP: {

			/* Get the key array */
			duckdb_logical_type keyType = duckdb_map_type_key_type(type);
			Oid keyOid = convertDuckType(NULL, keyType);
			duckdb_destroy_logical_type(&keyType);

			/* Get the value array */
			duckdb_logical_type valueType = duckdb_map_type_value_type(type);
			Oid valueOid = convertDuckType(NULL, valueType);
			duckdb_destroy_logical_type(&valueType);

			/* Create the MAP type */
			oid = convertMapType(name, keyOid, valueOid);
		} break;

		case DUCKDB_TYPE_STRUCT: {

			/* Figure out how many children, and size the arrays accordingly */
			idx_t nrChildren = duckdb_struct_type_child_count(type);
			char **names = malloc(nrChildren * sizeof(char *));
			Oid *oids = malloc(nrChildren * sizeof(Oid));

			/* Do for each child type of the struct */
			for (idx_t i = 0; i < nrChildren; i++) {

				/* Fetch the name of the child, or "" if no name. */
				names[i] = duckdb_struct_type_child_name(type, i);

				/* Fetch the type of the child */
				duckdb_logical_type childType = duckdb_struct_type_child_type(type, i);
				oids[i] = convertDuckType(NULL, childType);
				duckdb_destroy_logical_type(&childType);
			}

			/* Create an anonymous struct type */
			oid = convertStructType(name, nrChildren, names, oids);

			free(names);
			free(oids);
		} break;

		case DUCKDB_TYPE_LIST: {
			duckdb_logical_type childType = duckdb_list_type_child_type(type);
			Oid childOid = convertDuckType(name, childType);
			duckdb_destroy_logical_type(&childType);
			oid = convertListType(NULL, childOid);
		} break;

		case DUCKDB_TYPE_UNION: {

			/* Figure out how many children and size the arrays accordingly */
			idx_t nrChildren = duckdb_union_type_member_count(type);
			char **names = malloc(nrChildren * sizeof(char *));
			Oid *oids = malloc(nrChildren * sizeof(Oid));

			for (idx_t i = 0; i < nrChildren; i++) {

				/* Fetch the name of the child, or "" if no name. */
				names[i] = duckdb_union_type_member_name(type, i);

				/* Fetch the type of the child */
				duckdb_logical_type childType = duckdb_union_type_member_type(type, i);
				oids[i] = convertDuckType(NULL, childType);
				duckdb_destroy_logical_type(&childType);
			}

			/* Create an anonymous union type */
			oid = convertUnionType(name, nrChildren, names, oids);

			free(names);
			free(oids);

		} break;

		case DUCKDB_TYPE_DECIMAL: {
			uint8_t width = duckdb_decimal_width(type);
			uint8_t scale = duckdb_decimal_scale(type);
			oid = convertDecimalType(NULL, width, scale);
		} break;

		default:
			oid = convertDuckBaseType(NULL, typeid);
			break;
	}

	level--;

}
