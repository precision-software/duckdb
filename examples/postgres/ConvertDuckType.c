#include "duckdbconvert.h"
#include "duckdb.h"


#define debug printf

static Oid convertDuckBaseType(char *name, duckdb_type typeid);

static void indent();

static Oid convertMapType(char *name, Oid keyOid, Oid valueOid);
static Oid convertListType(char *name, Oid oid);
static Oid convertUnionType(char *name, int nrChildren, char **names, Oid *oids);
static Oid convertStructType(char *name, int nrChildren, char **names, Oid *oids);
static Oid convertDecimalType(char *name, int width, int scale);

/*
 * Convert a DuckDb type to a Postgres type.
 * Creates and reuses anonymous Postgres types as needed.
 */
Oid convertDuckType(char *name, duckdb_logical_type type) {

    level++;
	Oid oid;

    duckdb_type typeid = duckdb_get_type_id(type);
    switch (typeid) {

        case DUCKDB_TYPE_MAP: {

            /* Get the key type */
            duckdb_logical_type keyType = duckdb_map_type_key_type(type);
        	Oid keyOid = convertDuckType(NULL, keyType);
        	duckdb_destroy_logical_type(&keyType);

            /* Get the value type */
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

    return oid;
}



	static Oid convertMapType(char *name, Oid keyType, Oid valueType)
	{
	    indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("MAP(%d, %d)\n", keyType, valueType);
	    return -1;
	}

	static Oid convertListType(char *name, Oid type)
	{
	    indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("LIST\n");
	    return -2;
	}

	static Oid convertUnionType(char *name, int nrChildren, char **names, Oid *oids)
    {
    	indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("UNION\n");
    	return -3;
    }

	static Oid convertStructType(char *name, int nrChildren, char **names, Oid *oids)
    {
    	indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("STRUCT\n");
    	return -4;
    }

    static Oid convertDecimalType(char *name, int width, int scale)
    {
    	indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("DECIMAL(%d,%d)\n", width, scale);
        return -5;
    }

    static Oid convertDuckBaseType(char *name, duckdb_type typeId)
    {
        indent(); if (name != NULL && name[0] != '\0') debug("%s: ", name); debug("%s\n", typeName(typeId));
        return -9999;
    }


/*
 * Get the text name of a DuckDb type.
 */
const char* typeName(duckdb_type typeid)
{
	static const char *typeNames [] = {
		"INVALID",  /* 0 */
		"BOOLEAN",
		"TINYINT",
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"UTINYINT",
		"USMALLINT",
		"UINTEGER",
		"UBIGINT",
		"FLOAT",
		"DOUBLE",
		"TIMESTAMP",
		"DATE",
		"TIME",
		"INTERVAL",
		"HUGEINT",
		"VARCHAR",
		"BLOB",
		"DECIMAL",
		"TIMESTAMP_S",
		"TIMESTAMP_MS",
		"TIMESTAMP_NS",
		"ENUM",
		"LIST",
		"STRUCT",
		"MAP",
		"UUID",
		"UNION",
		"BIT",
		"TIME_TZ",
		"TIMESTAMP_TZ",
		"UHUGEINT",
		"ARRAY",
		};

    /* Assign "INVALID" if out of range. */
    if (typeid > sizeof(typeNames)/sizeof(*typeNames))
        typeid = 0;

    /* Return the string name of the type */
	return typeNames[typeid];
}
