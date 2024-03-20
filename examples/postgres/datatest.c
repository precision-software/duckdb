#include "duckdbconvert.h"
#include <stdio.h>
#include <stdlib.h>

typedef int Oid;

#define debug printf

static Oid convertDuckBaseType(char *name, duckdb_type typeid);

static const char* typeName(duckdb_type typeId);
static void indent();

static Oid createMapType(char *name, Oid keyOid, Oid valueOid);
static Oid createListType(char *name, Oid oid);
static Oid createUnionType(char *name, int nrChildren, char **names, Oid *oids);
static Oid createStructType(char *name, int nrChildren, char **names, Oid *oids);
static Oid createDecimalType(char *name, int width, int scale);


int main() {
	duckdb_database db = NULL;
	duckdb_connection con = NULL;
	duckdb_result result;

	if (duckdb_open(NULL, &db) == DuckDBError) {
		fprintf(stderr, "Failed to open database\n");
		goto cleanup;
	}
	if (duckdb_connect(db, &con) == DuckDBError) {
		fprintf(stderr, "Failed to open connection\n");
		goto cleanup;
	}

    const char *sql =
    	"SELECT {'birds':"
		"			{'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'},"
		"		'aliens':"
		"			NULL,"
		"		'amphibians':"
		"			{'yes':'frog', 'maybe': 'salamander', 'huh': 'dragon', 'no':'toad'},"
        "        'row': (1, 107.66, 3.5, 17),"
        "        'map': MAP(['abc', 'def', 'efg'], [1.23, 4.56, 7.99]::float[])"
    	"}";

	if (duckdb_query(con, sql, &result) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}

    /* What type of result did we get? */
    duckdb_result_type resultType = duckdb_result_return_type(result);

	/* Do for each chunk of data */
	idx_t nrChunks = duckdb_result_chunk_count(result);
    for (idx_t i = 0; i < nrChunks; i++) {
        duckdb_data_chunk chunk = duckdb_result_get_chunk(result, i);

        /* Get the shape of the chunk */
    	idx_t nrTuples = duckdb_data_chunk_get_size(chunk);
    	idx_t nrColumns = duckdb_data_chunk_get_column_count(chunk);
    	debug("Read chunk with %llu tuples and %llu columns\n", nrTuples, nrColumns);

        /* Do for each column */
        for (idx_t col = 0; col < nrColumns; col++) {

            duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, col);
            duckdb_logical_type type = duckdb_vector_get_column_type(vec);
            duckdb_type typeId = duckdb_get_type_id(type);

            debug("chunk(%llu)  col(%llu)  %s\n", i, col, typeName(typeId));

            duckdb_destroy_logical_type(&type);
            duckdb_destroy_data_chunk(&chunk);

        }
   }

	// duckdb_print_result(result);

cleanup:
	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}
