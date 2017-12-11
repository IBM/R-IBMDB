/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#include "dc.h"

namespace rdb2 {

typedef struct {
  Rcpp::IntegerVector integer_data;
  Rcpp::NumericVector numeric_data;
  Rcpp::StringVector string_data;

  short type;
} vector_var_data;


static std::vector<vector_var_data> __convert_vectors(const struct read_results& results) {
  size_t i;
  size_t ncols = results.col_desc.size();

  std::vector<vector_var_data> vecs(ncols);
  unsigned int nrows = results.null_indics[0].size();

  for (i = 0; i < ncols; i++) {
    vecs[i].type = results.stl_vecs[i].type;
    switch (results.stl_vecs[i].type) {
    case COLTYPE_STRING:
      vecs[i].string_data = results.stl_vecs[i].string_data;
      for (unsigned int j = 0; j < nrows; j++) {
        if (results.null_indics[i][j])
          vecs[i].string_data[j] = NA_STRING;
      }
      break;
    case COLTYPE_INTEGER:
      vecs[i].integer_data = results.stl_vecs[i].integer_data;
      for (unsigned int j = 0; j < nrows; j++) {
        if (results.null_indics[i][j])
          vecs[i].integer_data[j] = NA_INTEGER;
      }
      break;
    case COLTYPE_NUMERIC:
      vecs[i].numeric_data = results.stl_vecs[i].numeric_data;
      for (unsigned int j = 0; j < nrows; j++) {
        if (results.null_indics[i][j])
          vecs[i].numeric_data[j] = NA_REAL;
      }

      break;
    default:
      vecs[i].type = COLTYPE_STRING;
      vecs[i].string_data = results.stl_vecs[i].string_data;
    }
  }

  return vecs;
}


static std::vector<std::string> __get_col_names(const std::vector<column_desc>& col_desc) {
  size_t ncols = col_desc.size();
  size_t i;

  std::vector < std::string > col_names(ncols);
  std::ostringstream oss;

  oss.str(std::string());

  for (i = 0; i < ncols; i++) {
    oss << col_desc[i].colname;
    col_names[i] = oss.str();
    oss.str(std::string());
  }

  return col_names;
}

static Rcpp::DataFrame __make_DataFrame(Rcpp::List& list, bool strings_as_factors) {
  // create dataframe from Rcpp List
  // we do it manually so that we can set the stringsAsFactors parameter
  // without using DataFrame::create. Code snippet is based on DataFrame.h in Rcpp Github repo
  SEXP as_df_symb = Rf_install("as.data.frame");
  SEXP strings_as_factors_symb = Rf_install("stringsAsFactors");
  Rcpp::Shield < SEXP > call(Rf_lang3(as_df_symb, list, Rcpp::wrap(strings_as_factors)));
  SET_TAG(CDDR(call), strings_as_factors_symb);
  Rcpp::Shield<SEXP> df(Rcpp::Rcpp_eval (call));

  return df;
}

static Rcpp::DataFrame __get_DataFrame(const read_results& results, bool strings_as_factors) {
  // retrieve data and convert into R dataframe
  size_t ncols = results.col_desc.size();
  size_t i;
  Rcpp::List list(ncols);

  std::vector < std::string > col_names = __get_col_names(results.col_desc);
  std::vector<vector_var_data> vecs = __convert_vectors(results);
  list.attr("names") = col_names;

  if (vecs.size() > 0) {
    for (i = 0; i < ncols; i++) {
      if (vecs[i].type == COLTYPE_STRING) {
        list[i] = vecs[i].string_data;
      } else if (vecs[i].type == COLTYPE_NUMERIC) {
        list[i] = vecs[i].numeric_data;
      } else {
        list[i] = vecs[i].integer_data;
      }
    }
  }

  return __make_DataFrame(list, strings_as_factors);
}
}

using namespace rdb2;

//' @noRd
//' @export
// [[Rcpp::export(name=".dbExecuteQueryInternal")]]

Rcpp::DataFrame dbExecuteQueryInternal(const SEXP& handle, const std::string& query, unsigned int chunksize,
    bool stringsAsFactors) {

  SQLHDBC dbc = get_dbc_handle(handle);

  // push into STL vectors and then transfer into Rcpp vectors
  // as a two step process is OK because Rcpp explicitly recommends against
  // incremental push_back directly into Rcpp vectors (for performance reasons)
  struct read_results results = execute_query(dbc, query, chunksize);

  return __get_DataFrame(results, stringsAsFactors);
}
