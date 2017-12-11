/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#include "dc.h"

namespace rdb2 {

static std::string get_insert_SQL(std::string const& tbl_name, std::vector<std::string> const& col_names) {
  size_t i;
  std::ostringstream oss;
  size_t ncols = col_names.size();

  oss.str(std::string());
  oss << "INSERT INTO " << tbl_name << "(";
  oss << col_names[0] << " ";
  for (i = 1; i < ncols; i++) {
    oss << ", " << col_names[i] << " ";
  }
  oss << ") VALUES (?";
  for (i = 1; i < ncols; i++) {
    oss << ", ?";
  }
  oss << ")";

  return oss.str();
}

static std::vector<short> init_col_vectors(const Rcpp::DataFrame& df, const Rcpp::List& R_coltypes) {
  // returns coltypes array containing the column type for each column

  unsigned int i;
  unsigned int ncols = df.size();
  std::vector<short> coltypes(ncols);

  for (i = 0; i < ncols; i++) {
    if (strcmp(R_coltypes[i], "character") == 0) {
      coltypes[i] = COLTYPE_STRING;
    } else if (strcmp(R_coltypes[i], "integer") == 0) {
      coltypes[i] = COLTYPE_INTEGER;
    } else if (strcmp(R_coltypes[i], "numeric") == 0) {
      coltypes[i] = COLTYPE_NUMERIC;
    } else {
      throw std::runtime_error(std::string("Unknown column type: " + R_coltypes[i]));
    }
  }

  return coltypes;
}


static indic_arrays alloc_indic_mem(const unsigned long nrows, const size_t& ncols) {
  // allocate memory for null indicators

  indic_arrays null_indicator(ncols);
  size_t i;

  for (i = 0; i < ncols; i++) {
    std::unique_ptr<INDIC_TYPE[]> ptr(new INDIC_TYPE[nrows]);
    null_indicator[i] = std::move(ptr);
  }

  return null_indicator;
}

static data_arrays alloc_mem(std::vector<short> coltypes, const std::vector<int>& varchar_col_lengths,
    indic_arrays& null_indicator, const unsigned long nrows, const size_t& ncols) {

  // allocate memory for data - the ODBC driver requires that data be stored in contiguous memory
  // and unfortunately, this means we have to copy it over from the Rcpp::DataFrame that we received
  // specifically for strings, ODBC driver requires that all strings be stored as constant width
  // so we need to allocate a contiguous 2-D array of strings where every string occupies as much memory
  // as the longest string in that column!

  size_t i;
  data_arrays data(ncols);

  for (i = 0; i < ncols; i++) {
    if (coltypes[i] == COLTYPE_STRING) {
      SQLWCHAR* char_data = new SQLWCHAR[nrows * (varchar_col_lengths[i] + 1)];
      std::shared_ptr<SQLWCHAR> ptr(char_data, std::default_delete<SQLWCHAR[]>());
      colData cd(ptr);
      data[i] = std::move(cd);
    } else if (coltypes[i] == COLTYPE_INTEGER) {
      std::shared_ptr < SQLBIGINT > ptr(new SQLBIGINT[nrows], std::default_delete<SQLBIGINT[]>());
      colData cd(ptr);
      data[i] = std::move(cd);
    } else if (coltypes[i] == COLTYPE_NUMERIC) {
      std::shared_ptr < SQLDOUBLE > ptr(new SQLDOUBLE[nrows], std::default_delete<SQLDOUBLE[]>());
      colData cd(ptr);
      data[i] = std::move(cd);
    }
  }

  return data;
}

static void copy_data(const Rcpp::DataFrame& df, data_arrays& data, indic_arrays& null_indicator,
    const std::vector<int>& varchar_col_lengths, const std::vector<short>& coltypes, const unsigned long nrows,
    const size_t ncols) {
  // Now update the parameter values and write the rows
  // note that assigning dataframe column to an Rcpp::CharacterVector, NumericVector etc.
  // does not make a copy of the column per
  // http://lists.r-forge.r-project.org/pipermail/rcpp-devel/2013-February/005332.html
  // However, we do manually copy the data because ODBC SQLBindParameter needs the data to
  // be stored contiguously in memory. The conversion to UTF-16 executes a copy internally anyway so
  // this copy ends up being unavoidable for strings for more than one reason. We provide the contiguous memory
  // location to the UTF-16 conversion function so we only copy once, not twice.

  size_t i;

  for (i = 0; i < ncols; i++) {
    Rcpp::checkUserInterrupt();   // allow user interrupts

    if (coltypes[i] == COLTYPE_STRING) {
      Rcpp::CharacterVector cv = df[i];
      for (unsigned int j = 0; j < nrows; j++) {
        if (Rcpp::CharacterVector::is_na(cv[j])) {
          null_indicator[i][j] = SQL_NULL_DATA;
          encodeUTF8StringAsUTF16((SQLWCHAR*) (data[i].get()) + j * (varchar_col_lengths[i] + 1), "");
        } else {
          std::string char_data = Rcpp::as<std::string>(cv[j]);
          encodeUTF8StringAsUTF16((SQLWCHAR*) (data[i].get()) + j * (varchar_col_lengths[i] + 1), char_data);
          null_indicator[i][j] = SQL_NTS;
        }
      }
    } else if (coltypes[i] == COLTYPE_INTEGER) {
      Rcpp::IntegerVector iv = df[i];
      for (unsigned int j = 0; j < nrows; j++) {
        if (Rcpp::IntegerVector::is_na(iv[j])) {
          null_indicator[i][j] = SQL_NULL_DATA;
        } else {
          *((SQLBIGINT*) (data[i].get()) + j) = (SQLBIGINT)(iv[j]);
          null_indicator[i][j] = 0;
        }
      }
    } else if (coltypes[i] == COLTYPE_NUMERIC) {
      Rcpp::NumericVector nv = df[i];
      for (unsigned int j = 0; j < nrows; j++) {
        if (Rcpp::NumericVector::is_na(nv[j])) {
          null_indicator[i][j] = SQL_NULL_DATA;
        } else {
          *((SQLDOUBLE*) (data[i].get()) + j) = (SQLDOUBLE)(nv[j]);
          null_indicator[i][j] = 0;
        }
      }
    }
  }
}

} // namespace

using namespace rdb2;

//' @noRd
//' @export
// [[Rcpp::export(name=".dbWriteTableInternal")]]

void dbWriteTableInternal(const SEXP& handle, const Rcpp::DataFrame& df, const std::string& tbl_name,
    const std::vector<std::string>& col_names, const Rcpp::List& R_coltypes,
    const std::vector<int>& varchar_col_lengths, const bool& verbose) {

  SQLHDBC dbc = get_dbc_handle(handle);

  size_t ncols = df.size(); /* number of columns in dataframe */
  unsigned long nrows = df.nrows();

  std::vector<short> coltypes = init_col_vectors(df, R_coltypes);

  std::string insert_SQL = get_insert_SQL(tbl_name, col_names);

  if (verbose)
    Rcpp::Rcout << insert_SQL << std::endl;

  indic_arrays null_indicator = alloc_indic_mem(nrows, ncols);

  data_arrays data = alloc_mem(coltypes, varchar_col_lengths, null_indicator, nrows, ncols);

  // Now copy the data to contiguous memory for SQLBindParameter
  copy_data(df, data, null_indicator, varchar_col_lengths, coltypes, nrows, ncols);

  write_table(dbc, tbl_name, insert_SQL, data, null_indicator, coltypes, varchar_col_lengths, nrows);
}

