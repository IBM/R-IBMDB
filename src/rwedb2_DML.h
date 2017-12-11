
/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2_DML.h
 *
 *  Created on: Mar 21, 2017
 *      Author: karthik
 */

#ifndef SRC_RWEDB2_DML_H_
#define SRC_RWEDB2_DML_H_

#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <memory>
#include <string>

#include "rwedb2_utils.h"

#define COLTYPE_STRING 0
#define COLTYPE_INTEGER 1
#define COLTYPE_NUMERIC 2

/* below is workaround for the fact that indicator type in
 # SQLBindParameter and SQLBindCol is supposed to be SQLLEN (64-bit)
 # but DB2 driver has some odd specification for SQLBindParameter and actually returns SQLINTEGER (32-bit)
 # change definition below to SQLLEN if driver gets fixed
 */
#define INDIC_TYPE SQLINTEGER

namespace rdb2 {
typedef struct {
  SQLCHAR colname[128]; // max column name length in DB2 is 128 bytes
  SQLCHAR coltype[32]; // column type as a string
  SQLINTEGER type;  // column concise type from SQL_DESC_CONCISE_TYPE
  SQLLEN displaysize;
  SQLLEN precision;
  SQLLEN scale;
} column_desc;

struct stl_vector_var_data {
  std::vector<SQLINTEGER> integer_data;
  std::vector<SQLDOUBLE> numeric_data;
  std::vector<std::string> string_data;
  short type;
};

struct read_results {
  std::vector<struct stl_vector_var_data> stl_vecs;
  std::vector<std::vector<bool>> null_indics;
  std::vector<column_desc> col_desc;
  
  read_results() {}
  
  read_results(int ncols) {
  	stl_vecs.resize(ncols);
  	null_indics.resize(ncols);
  	col_desc.resize(ncols);
  }

  size_t get_col_index_from_name(const std::string& name) {
    // returns index of column with the given name
    for (size_t i = 0; i < col_desc.size(); i++) {
      if (reinterpret_cast<const char*> (col_desc[i].colname) == name) {
        return i;
      }
    }
    return ((size_t) -1);
  }
};

typedef std::vector<std::unique_ptr<INDIC_TYPE[]>> indic_arrays;

typedef struct colData {
  // used when writing to DB to store shared_ptr
  // to contiguous memory array storing the data for a single column
  // TODO: find a better way to initialize this
private:
  std::shared_ptr<SQLWCHAR> charVals;
  std::shared_ptr<SQLBIGINT> intVals;
  std::shared_ptr<SQLDOUBLE> doubleVals;

public:
  enum data_type {
    DT_CHAR, DT_BIGINT, DT_DOUBLE
  } type;

  colData() {
  }

  colData(std::shared_ptr<SQLWCHAR> cPtr) :
      charVals(cPtr), type(DT_CHAR) {
  }

  colData(std::shared_ptr<SQLBIGINT> iPtr) :
      intVals(iPtr), type(DT_BIGINT) {
  }

  colData(std::shared_ptr<SQLDOUBLE> dPtr) :
      doubleVals(dPtr), type(DT_DOUBLE) {
  }

  SQLPOINTER get() {
    SQLPOINTER retval = NULL;
    switch (type) {
    case DT_CHAR:
      retval = (SQLPOINTER) charVals.get();
      break;
    case DT_BIGINT:
      retval = (SQLPOINTER) intVals.get();
      break;
    case DT_DOUBLE:
      retval = (SQLPOINTER) doubleVals.get();
      break;
    }
    return retval;
  }
} colData;

typedef std::vector<colData> data_arrays;

struct read_results execute_query(const SQLHDBC& handle, const std::string& query, unsigned int chunksize = 1);

void write_table(const SQLHDBC& dbc, const std::string& tbl_name, const std::string& insert_SQL, data_arrays& data,
    indic_arrays& null_indicator, const std::vector<short>& coltypes, const std::vector<int>& varchar_col_lengths,
    unsigned long nrows);
}


#endif /* SRC_RWEDB2_DML_H_ */
