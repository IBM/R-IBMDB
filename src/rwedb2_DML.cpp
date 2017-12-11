
/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2_DML.cpp
 *
 *  Created on: Mar 21, 2017
 *      Author: karthik
 */
#include "rwedb2.h"
#include <iostream>
#include <sstream>
#include <cstring>

#include "utf8.h"

#define DATE_FIELD_MIN_LENGTH 32

// width of DECFLOAT field is precision (34 for DECFLOAT) + 2 (for +/- sign and decimal point)
// + 1 for terminating null
#define DECFLOAT_FIELD_WIDTH 37

// place holder values to represent NULL's.
// the real null indicator is in the null_indic array and 
// callers of this library should rely on that to identify NULL values

#define DUMMY_INT 0
#define DUMMY_STRING ""

namespace rdb2 {

/***************************************************
 Functions to support dbReadTable 
****************************************************/

static std::vector<column_desc> __get_column_descs(SQLHSTMT stmt, size_t ncols) {
  std::vector<column_desc> col_desc(ncols);
  size_t i;
  SQLRETURN ret; /* ODBC API return status */

  for (i = 0; i < ncols; i++) {
    if (!SQL_SUCCEEDED(
        ret = SQLColAttribute(stmt, i + 1, SQL_DESC_NAME, col_desc[i].colname, sizeof(col_desc[i].colname), NULL,
            NULL))) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
    }

    if (!SQL_SUCCEEDED(
        ret = SQLColAttribute(stmt, i + 1, SQL_DESC_TYPE_NAME, col_desc[i].coltype, sizeof(col_desc[i].coltype), NULL,
            NULL))) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
    }

    col_desc[i].displaysize = 0;
    if (!SQL_SUCCEEDED(
        ret = SQLColAttribute(stmt, i + 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &col_desc[i].displaysize))) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
    }

    col_desc[i].type = 0;
    if (!SQL_SUCCEEDED(
        ret = SQLColAttribute(stmt, i + 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, (SQLLEN*) &col_desc[i].type))) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
    }

    col_desc[i].precision = 0;
    ret = SQLColAttribute(stmt, i + 1, SQL_DESC_PRECISION, NULL, 0, NULL, &(col_desc[i].precision));
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
    }

  }

  return col_desc;
}

static void __bind_cols(SQLHSTMT stmt, const std::vector<column_desc>& col_desc, std::vector<std::shared_ptr<void>>& data,
    std::vector<std::unique_ptr<SQLLEN[]>>& indicator, unsigned int chunksize) {
  size_t i;
  int date_field_size;
  SQLRETURN ret;
  size_t ncols = col_desc.size();
  
  for (i = 0; i < ncols; i++) {
    switch (col_desc[i].type) {
    case SQL_CHAR:
    case SQL_VARCHAR: {
      SQLWCHAR* char_data = new SQLWCHAR[chunksize * (col_desc[i].displaysize + 1)];
      data[i] = std::shared_ptr < SQLWCHAR >(char_data, std::default_delete<SQLWCHAR[]>());

      ret = SQLBindCol(stmt, i + 1, SQL_C_WCHAR, (SQLPOINTER) data[i].get(), sizeof(SQLWCHAR) * (col_desc[i].displaysize + 1),
                       indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_INTEGER: {
      data[i] = std::shared_ptr < SQLINTEGER >(new SQLINTEGER[chunksize], std::default_delete<SQLINTEGER[]>());
      ret = SQLBindCol(stmt, i + 1, SQL_C_LONG, data[i].get(), sizeof(SQLINTEGER), indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_SMALLINT: {
      data[i] = std::shared_ptr < SQLSMALLINT >(new SQLSMALLINT[chunksize], std::default_delete<SQLSMALLINT[]>());
      ret = SQLBindCol(stmt, i + 1, SQL_C_SHORT, data[i].get(), sizeof(SQLSMALLINT), indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_BIGINT: {
      data[i] = std::shared_ptr < SQLBIGINT >(new SQLBIGINT[chunksize], std::default_delete<SQLBIGINT[]>());
      ret = SQLBindCol(stmt, i + 1, SQL_C_SBIGINT, data[i].get(), sizeof(SQLBIGINT), indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_DECIMAL:
    case SQL_NUMERIC: {
      // width of numeric field is precision + 2 (for +/- sign and decimal point) + 1 for terminating null
      short field_width = col_desc[i].precision + 3;
      SQLWCHAR* numeric_data = new SQLWCHAR[chunksize * field_width];
      data[i] = std::shared_ptr < SQLWCHAR > (numeric_data, std::default_delete<SQLWCHAR[]>());

      ret = SQLBindCol(stmt, i + 1, SQL_C_WCHAR, (SQLPOINTER) data[i].get(),
          sizeof(SQLWCHAR) * field_width, indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_DECFLOAT: {
      short field_width = DECFLOAT_FIELD_WIDTH;
      SQLWCHAR* numeric_data = new SQLWCHAR[chunksize * field_width];
      data[i] = std::shared_ptr < SQLWCHAR > (numeric_data, std::default_delete<SQLWCHAR[]>());

      ret = SQLBindCol(stmt, i + 1, SQL_C_WCHAR, (SQLPOINTER) data[i].get(),
          sizeof(SQLWCHAR) * field_width, indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_TYPE_DATE:
    case SQL_TYPE_TIME:
    case SQL_TYPE_TIMESTAMP: {
      date_field_size =
          (col_desc[i].displaysize > DATE_FIELD_MIN_LENGTH) ? col_desc[i].displaysize : DATE_FIELD_MIN_LENGTH;
      SQLWCHAR* char_data = new SQLWCHAR[chunksize * date_field_size];
      data[i] = std::shared_ptr < SQLWCHAR >(char_data, std::default_delete<SQLWCHAR[]>());

      ret = SQLBindCol(stmt, i + 1, SQL_C_WCHAR, (SQLPOINTER) data[i].get(), sizeof(SQLWCHAR) * date_field_size,
                       indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    case SQL_DOUBLE:
    case SQL_REAL:
    case SQL_FLOAT: {
      data[i] = std::shared_ptr < SQLDOUBLE >(new SQLDOUBLE[chunksize], std::default_delete<SQLDOUBLE[]>());
      ret = SQLBindCol(stmt, i + 1, SQL_C_DOUBLE, data[i].get(), sizeof(SQLDOUBLE), indicator[i].get());
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLBindCol in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
      break;
    }
    default:
      throw std::runtime_error(std::string("Unable to bind column of type ") + 
      						reinterpret_cast<const char*>(col_desc[i].coltype));
    }
  }
}

void process_string_col(struct read_results& result, SQLROWSETSIZE row_count, const size_t i, int field_width,
    const std::vector<std::shared_ptr<void>>& data, const std::vector<column_desc>& col_desc, INDIC_TYPE* indic) {
  long offset = 0;
  size_t j;

  result.stl_vecs[i].type = COLTYPE_STRING;
  for (j = 0; j < row_count; j++) {
    if (indic[j] == SQL_NULL_DATA) {
      result.stl_vecs[i].string_data.push_back(DUMMY_STRING);
      result.null_indics[i].push_back(true);
    } else {
      std::string utf8line = encodeUTF16StringAsUTF8((SQLWCHAR*) (data[i].get()) + offset, indic[j]);
      result.stl_vecs[i].string_data.push_back(utf8line);
      result.null_indics[i].push_back(false);
    }
    offset += field_width;
  }
}

static read_results __fetch_data(SQLHSTMT stmt, const std::vector<column_desc>& col_desc, unsigned int chunksize) {
  size_t i;
  size_t j;
  SQLRETURN ret;
  INDIC_TYPE* indic;
  short field_width;

  size_t ncols = col_desc.size();

  SQLROWSETSIZE row_count_param;
  SQLUINTEGER row_count;

  std::unique_ptr<SQLUSMALLINT[]> row_status(new SQLUSMALLINT[chunksize]);
  
  std::vector<std::unique_ptr<SQLLEN[]>> indicator(ncols);
  std::vector<std::shared_ptr<void>> data(ncols);
  struct read_results result(ncols);

  for (i = 0; i < ncols; i++) {
    indicator[i] = std::unique_ptr<SQLLEN[]>(new SQLLEN[chunksize]);
    // not sure why the below zero-initialization line is needed but the read will fail without it
    std::memset(indicator[i].get(), 0, sizeof(SQLLEN) * chunksize);
  }

  ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &row_count_param, 0 );
  if (!SQL_SUCCEEDED(ret)) {
    throw std::runtime_error(
        extract_error("Error setting row count parameter in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
  }

  ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, row_status.get(), 0);
  if (!SQL_SUCCEEDED(ret)) {
    throw std::runtime_error(
        extract_error("Error setting row status parameter in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
  }

  result.col_desc = col_desc;
  __bind_cols(stmt, col_desc, data, indicator, chunksize);

  while (SQL_SUCCEEDED(ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0))) {
    // DB2 actually returns a 32-bit value in row_count_param
    // so we have to cast it to SQLUINTEGER
    row_count = (SQLUINTEGER) row_count_param;
    for (j = 0; j < row_count; j++) {
      if (row_status[j] != SQL_SUCCESS && row_status[j] != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error(
            extract_error("Error " + std::to_string(row_status[j]) + " when reading row " + std::to_string(j)
                          + " in " + std::string(__func__), stmt, SQL_HANDLE_STMT));
      }
    }

    checkInterrupt();

  /* Loop through the ncols */
    for (i = 0; i < ncols; i++) {
      indic = (INDIC_TYPE*) indicator[i].get();
      switch (col_desc[i].type) {
      case SQL_CHAR:
      case SQL_VARCHAR:
        field_width = col_desc[i].displaysize + 1;
        process_string_col(result, row_count, i, field_width, data, col_desc, indic);
        break;
      case SQL_DECIMAL:
      case SQL_NUMERIC:
        field_width = col_desc[i].precision + 3;
        process_string_col(result, row_count, i, field_width, data, col_desc, indic);
        break;
      case SQL_DECFLOAT:
        field_width = DECFLOAT_FIELD_WIDTH;
        process_string_col(result, row_count, i, field_width, data, col_desc, indic);
        break;
      case SQL_TYPE_DATE:
      case SQL_TYPE_TIME:
      case SQL_TYPE_TIMESTAMP:
        field_width = (col_desc[i].displaysize > DATE_FIELD_MIN_LENGTH) ?
                      col_desc[i].displaysize : DATE_FIELD_MIN_LENGTH;
        process_string_col(result, row_count, i, field_width, data, col_desc, indic);
        break;
      case SQL_INTEGER:
        result.stl_vecs[i].type = COLTYPE_INTEGER;
        for (j = 0; j < row_count; j++) {
          if ((SQLINTEGER) indic[j] == SQL_NULL_DATA) {
            result.null_indics[i].push_back(true);
            result.stl_vecs[i].integer_data.push_back(DUMMY_INT);
          } else {
            SQLINTEGER intVal = *((SQLINTEGER*) data[i].get() + j);
            result.null_indics[i].push_back(false);
            result.stl_vecs[i].integer_data.push_back(intVal);
          }
        }
        break;
      case SQL_SMALLINT:
        result.stl_vecs[i].type = COLTYPE_INTEGER;
        for (j = 0; j < row_count; j++) {
          if ((SQLINTEGER) indic[j] == SQL_NULL_DATA) {
            result.null_indics[i].push_back(true);
            result.stl_vecs[i].integer_data.push_back(DUMMY_INT);
          } else {
            SQLSMALLINT intVal = *((SQLSMALLINT*) data[i].get() + j);
            result.null_indics[i].push_back(false);
            result.stl_vecs[i].integer_data.push_back(intVal);
          }
        }
        break;
      case SQL_BIGINT:
        // R only has 32-bit integers so we treat BIGINT as a DOUBLE type
        result.stl_vecs[i].type = COLTYPE_NUMERIC;
        for (j = 0; j < row_count; j++) {
          if ((SQLINTEGER) indic[j] == SQL_NULL_DATA) {
            result.null_indics[i].push_back(true);
            result.stl_vecs[i].numeric_data.push_back(DUMMY_INT);
          } else {
            SQLBIGINT dblVal = *((SQLBIGINT*) data[i].get() + j);
            result.null_indics[i].push_back(false);
            result.stl_vecs[i].numeric_data.push_back(dblVal);
          }
        }
        break;
      case SQL_REAL:
      case SQL_DOUBLE:
      case SQL_FLOAT:
        result.stl_vecs[i].type = COLTYPE_NUMERIC;
        for (j = 0; j < row_count; j++) {
          if ((SQLINTEGER) indic[j] == SQL_NULL_DATA) {
            result.null_indics[i].push_back(true);
            result.stl_vecs[i].numeric_data.push_back(DUMMY_INT);
          } else {
            SQLDOUBLE dblVal = *((SQLDOUBLE*) data[i].get() + j);
            result.null_indics[i].push_back(false);
            result.stl_vecs[i].numeric_data.push_back(dblVal);
          }
        }
        break;
      default:
        result.stl_vecs[i].type = COLTYPE_STRING;
        for (j = 0; j < row_count; j++) {
          result.stl_vecs[i].string_data.push_back(std::string("Unknown column type: "));
        }
        break;
      }
    }
  }

  if (ret != SQL_NO_DATA) {
    throw std::runtime_error(
        extract_error("Error in " + std::string(__func__) + " while reading", stmt, SQL_HANDLE_STMT));
  }

  return result;
}

struct read_results execute_query(const SQLHDBC& dbc, const std::string& query, unsigned int chunksize) {
  
  struct odbc_stmt_handle stmt_holder;
  SQLRETURN ret; /* ODBC API return status */
  SQLSMALLINT ncols = 0; /* number of columns in result-set */

  /* Allocate a statement handle */
  if (!SQL_SUCCEEDED(ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_holder.stmt))) {
    throw std::runtime_error(
        extract_error("Error in " + std::string(__func__) + " while allocating statement", dbc, SQL_HANDLE_DBC));
  }

  if (!SQL_SUCCEEDED(
      ret = SQLSetStmtAttr(stmt_holder.stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) (long) chunksize, SQL_IS_INTEGER))) {
    throw std::runtime_error(
        extract_error("Error in SQLSetStmtAttr in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
  }


  if (!SQL_SUCCEEDED(ret = SQLExecDirectW(stmt_holder.stmt, get_UTF16_string(query).get(), SQL_NTS))) {
    throw std::runtime_error(
        extract_error("Error in SQLExecDirect in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT).c_str());
  }

  if (!SQL_SUCCEEDED(ret = SQLNumResultCols(stmt_holder.stmt, &ncols))) {
    throw std::runtime_error(
        extract_error("Error in SQLNumResultCols in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
  }

  std::vector<column_desc> col_descs = __get_column_descs(stmt_holder.stmt, ncols);

  struct read_results results = __fetch_data(stmt_holder.stmt, col_descs, chunksize);

  return results;
}

/***************************************************
 Functions to support dbWriteTable 
****************************************************/

static void __get_db_coltypes(const SQLHDBC& dbc, const std::string& tbl_name, 
							std::unique_ptr<column_desc[]>& col_desc, const size_t& ncols) {
  /* Get the column types for each column in the table. We do it by executing a simple select
   * query instead of SQLColumns so that it works for temporary tables also (since they are not
   * in the system catalog)
   */

  struct odbc_stmt_handle stmt_holder;
  SQLRETURN ret;
  SQLSMALLINT columns = 0; /* number of columns in result-set */
  size_t i;

  std::string error;
  std::string query = "SELECT * FROM " + tbl_name + " FETCH FIRST 1 ROWS ONLY";

  /* Allocate a statement handle */
  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_holder.stmt))) {
    error = extract_error("Error while allocating statement handle getting column types", dbc, SQL_HANDLE_DBC);
    throw std::runtime_error(error);
  }

  if (!SQL_SUCCEEDED(ret = SQLExecDirectW(stmt_holder.stmt, get_UTF16_string(query).get(), SQL_NTS))) {
    throw std::runtime_error(
        extract_error("Error in SQLExecDirect in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
  }

  if (!SQL_SUCCEEDED(ret = SQLNumResultCols(stmt_holder.stmt, &columns))) {
    throw std::runtime_error(
        extract_error(std::string("Error in SQLNumResultCols in exec_query_internal"), stmt_holder.stmt,
            SQL_HANDLE_STMT));
  }

  if ((size_t) columns != ncols) {
    std::ostringstream oss;
    oss << "Expected " << ncols << " in " << tbl_name << " but found " << columns << " in " << __func__ << std::endl;
    throw std::runtime_error(oss.str());
  }

  for (i = 0; i < ncols; i++) {

    checkInterrupt();

    ret = SQLColAttribute(stmt_holder.stmt, i + 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, (SQLLEN*) &(col_desc[i].type));
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error(
          extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
    }

    if (col_desc[i].type == SQL_DECIMAL || col_desc[i].type == SQL_NUMERIC || col_desc[i].type == SQL_DECFLOAT) {
      ret = SQLColAttribute(stmt_holder.stmt, i + 1, SQL_DESC_PRECISION, NULL, 0, NULL, &(col_desc[i].precision));
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
      }

      ret = SQLColAttribute(stmt_holder.stmt, i + 1, SQL_DESC_SCALE, NULL, 0, NULL, &(col_desc[i].scale));
      if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error(
            extract_error("Error in SQLColAttribute in " + std::string(__func__), stmt_holder.stmt, SQL_HANDLE_STMT));
      }
    } else {
      col_desc[i].precision = 0;
      col_desc[i].scale = 0;
    }
  }
}

static void __bind_params(const SQLHDBC& dbc, const SQLHSTMT& stmt, const std::string& tbl_name, data_arrays& data,
    indic_arrays& null_indicator, const std::vector<short>& coltypes, const std::vector<int>& varchar_col_lengths) {

  size_t i;
  size_t ncols = coltypes.size();
  std::string error = "";
  std::unique_ptr<column_desc[]> col_desc(new column_desc[ncols]);

  __get_db_coltypes(dbc, tbl_name, col_desc, ncols);

  for (i = 0; i < ncols; i++) {
    checkInterrupt();

    if (coltypes[i] == 0) {
      if (!SQL_SUCCEEDED(
          SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_WCHAR, col_desc[i].type, col_desc[i].precision,
              col_desc[i].scale, data[i].get(), sizeof(SQLWCHAR) * (varchar_col_lengths[i] + 1), (SQLLEN*) (null_indicator[i].get())))) {
        error = extract_error("Error while setting up char parameter binding", stmt, SQL_HANDLE_STMT);
        throw std::runtime_error(error);
      }
    } else if (coltypes[i] == 1) {
      if (!SQL_SUCCEEDED(
          SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, col_desc[i].type, col_desc[i].precision,
              col_desc[i].scale, data[i].get(), 0, (SQLLEN*) (null_indicator[i].get())))) {
        error = extract_error("Error while setting up integer parameter binding", stmt, SQL_HANDLE_STMT);
        throw std::runtime_error(error);
      }
    } else if (coltypes[i] == 2) {
      if (!SQL_SUCCEEDED(
          SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, col_desc[i].type, col_desc[i].precision,
              col_desc[i].scale, data[i].get(), 0, (SQLLEN*) (null_indicator[i].get())))) {
        error = extract_error("Error while setting up SQL_DOUBLE parameter binding", stmt, SQL_HANDLE_STMT);
        throw std::runtime_error(error);
      }
    }
  }
}

static void __set_stmt_attributes(SQLHSTMT stmt, unsigned long nrows) {
  // TODO: add param status array. See https://msdn.microsoft.com/en-us/library/ms711818%28v=vs.85%29.aspx
  SQLRETURN ret; /* ODBC API return status */
  std::string error = "";

  if (!SQL_SUCCEEDED(ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0))) {
    error = extract_error("Error while setting up parameter binding", stmt, SQL_HANDLE_STMT);
    throw std::runtime_error(error);
  }

  if (!SQL_SUCCEEDED(ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(nrows), 0))) {
    error = extract_error("Error while setting paramset size", stmt, SQL_HANDLE_STMT);
    throw std::runtime_error(error);
  }
}

void write_table(const SQLHDBC& dbc, const std::string& tbl_name, const std::string& insert_SQL, data_arrays& data,
    indic_arrays& null_indicator, const std::vector<short>& coltypes, const std::vector<int>& varchar_col_lengths,
    unsigned long nrows) {

  SQLRETURN ret; /* ODBC API return status */
  std::string error = "";
  struct odbc_stmt_handle stmt_holder;

  SQLINTEGER autocommit;

  if (!SQL_SUCCEEDED(ret = SQLGetConnectAttr(dbc, SQL_AUTOCOMMIT, &autocommit, 0, NULL))) {
    throw std::runtime_error(extract_error("Error retrieving autocommit parameter", dbc, SQL_HANDLE_DBC));
  }

  // set autocommit to false
  if (!SQL_SUCCEEDED(ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_OFF, 0))) {
    throw std::runtime_error(extract_error("Error turning autocommit off", dbc, SQL_HANDLE_DBC));
  }

  /* Allocate a statement handle */
  if (!SQL_SUCCEEDED(ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_holder.stmt))) {
    error = extract_error("Error while allocating statement handle", dbc, SQL_HANDLE_DBC);
    throw std::runtime_error(error);
  }

  if (!SQL_SUCCEEDED(ret = SQLPrepareW(stmt_holder.stmt, get_UTF16_string(insert_SQL).get(), SQL_NTS))) {
    error = extract_error("Error while preparing statement ", dbc, SQL_HANDLE_DBC);
    throw std::runtime_error(error);
  }

  __set_stmt_attributes(stmt_holder.stmt, nrows);

  __bind_params(dbc, stmt_holder.stmt, tbl_name, data, null_indicator, coltypes, varchar_col_lengths);

  if (!SQL_SUCCEEDED(SQLExecute(stmt_holder.stmt))) {
    error = extract_error("Error in SQLExecute in dbWriteTableInternal", stmt_holder.stmt, SQL_HANDLE_STMT);
    if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK))) {
      error = error + " " + extract_error("Error in SQLExecute in dbWriteTableInternal - "
          "could not rollback. Table may be in corrupted state.", dbc, SQL_HANDLE_DBC);
    }
    throw std::runtime_error(error);
  }

  if (!SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT))) {
    throw std::runtime_error(
        extract_error("Error in SQLExecute in dbWriteTable - could not commit transaction.", dbc, SQL_HANDLE_DBC).c_str());
  }

  // reset autocommit to original value
  if (!SQL_SUCCEEDED(ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) (long) autocommit, 0))) {
    throw std::runtime_error(extract_error("Error restoring autocommit parameter", dbc, SQL_HANDLE_DBC));
  }
    	
}

} // namespace

