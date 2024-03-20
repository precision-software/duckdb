#ifndef DUCKDBCONVERT_LIBRARY_H
#define DUCKDBCONVERT_LIBRARY_H

#include <stdio.h>
#include <stdlib.h>

#include "duckdb.h"

typedef int Oid;
typedef void *Datum;


extern Oid convertDuckType(char *name, duckdb_logical_type type);
extern const char* typeName(duckdb_type typeId);
extern Datum getDuckData(duckdb_logical_type type, duckdb_data_chunk chunk, idx_t row, idx_t col);


/*
 * Used for debug output
 */
#define debug printf
static int level = 0;
static void indent()
{
	for (int i=0; i<level; i++)
		debug("   ");
}



#endif
