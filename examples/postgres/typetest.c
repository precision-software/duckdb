/*
 *
 */
#include <stdio.h>
#include <stdlib.h>

#include "duckdbconvert.h"


#define debug printf

/*
 * A quick test program to explore converting DuckDb types to Postgres types.
 * This version scans the DuckDb logical type tree rather than parsing the type strings.
 */
int main() {
	duckdb_database db = NULL;
	duckdb_connection con = NULL;
	duckdb_result result;

	/* Open and connect to a new DuckDb database */
	if (duckdb_open(NULL, &db) == DuckDBError) {
		fprintf(stderr, "Failed to open database\n");
		goto cleanup;
	}
	if (duckdb_connect(db, &con) == DuckDBError) {
		fprintf(stderr, "Failed to open connection\n");
		goto cleanup;
	}

	/* An SQL statement with xxxx types */
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

	// print the names of the result
	idx_t row_count = duckdb_row_count(&result);
	idx_t column_count = duckdb_column_count(&result);
	debug("\nColumn Names  ... row_count=%llu  column_count=%llu\n", row_count, column_count);
	for (idx_t i = 0; i < column_count; i++) {
		printf("%s ", duckdb_column_name(&result, i));
	}
	printf("\n");

	/* Convert the type for each of the columns */
	for (idx_t i = 0; i < column_count; i++) {
		duckdb_logical_type columnType = duckdb_column_logical_type(&result, i);
		debug("Column %llu\n", i);
		convertDuckType(NULL, columnType);
		duckdb_destroy_logical_type(&columnType);
	}

	/* print the data of the result */
	for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
		for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
			char *val = duckdb_value_varchar(&result, col_idx, row_idx);
			printf("%s ", val);
			duckdb_free(val);
		}
		printf("\n");
	}

	// duckdb_print_result(result);

	cleanup:
	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}
