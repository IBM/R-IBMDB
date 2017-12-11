/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#include "dc.h"
#include <Rdefines.h>

#define R_CALLOC Calloc   // R safe version of calloc - part of R's C interface
#define R_FREE Free       // R safe version of free - part of R's C interface

// [[Rcpp::interfaces(r, cpp)]]

namespace rdb2 {
unsigned int write_chunk_size = 1000000;    // write chunk size expressed as number of rows
unsigned int read_chunk_size = 1;			// read chunk size expressed as number of rows

long login_timeout = 120;  // login timeout in seconds
long connection_timeout = 120;  // connection timeout in seconds

typedef struct rdb2Handle {
  SQLHDBC dbc;
  SEXP handle_ptr;
} ODBCHandle, *pODBCHandle;

pODBCHandle __get_handle_from_R_handle(const SEXP& R_handle) {
  SEXP ptr = Rf_getAttrib(R_handle, Rf_install("handle_ptr"));
  if (!R_ExternalPtrAddr(ptr))
    return NULL;

  pODBCHandle handle = (pODBCHandle) R_ExternalPtrAddr(ptr);

  return handle;
}

static void __close_handle(pODBCHandle handle) {

  if (handle == NULL)
    return;

  SQLHDBC dbc = handle->dbc;
  rdb2::closeConn(dbc, true);

  R_ClearExternalPtr(handle->handle_ptr); /* make it NULL */
  R_FREE(handle);
}

static void handleFinalizer(SEXP ptr) {
  if (!R_ExternalPtrAddr(ptr))
    return;

  pODBCHandle handle = (pODBCHandle) R_ExternalPtrAddr(ptr);

  __close_handle(handle);
}

SEXP get_R_handle_from_SQL_handle(const SQLHDBC& dbc) {
  pODBCHandle handle;
  SEXP ans;

  // Make external ptr and return it to R
  handle = R_CALLOC(1, ODBCHandle);
  handle->dbc = dbc;
  SEXP ptr = R_MakeExternalPtr(handle, R_NilValue, R_NilValue);
  PROTECT(ptr);
  R_RegisterCFinalizerEx(ptr, handleFinalizer, TRUE);
  handle->handle_ptr = ptr;

  // Wrap it in a list attribute before sending it back to R
  // TODO: Test with standalone RDB2 pkg if sending it back
  // as a raw SEXP containing just the ptr will work
  PROTECT(ans = Rf_allocVector(INTSXP, 1));
  INTEGER(ans)[0] = 1;

  Rf_setAttrib(ans, Rf_install("handle_ptr"), ptr);
  R_PreserveObject(ans);
  UNPROTECT(2);

  return ans;
}

SQLHDBC get_dbc_handle(const SEXP& R_handle) {

  pODBCHandle handle = __get_handle_from_R_handle(R_handle);

  if (handle == NULL) {
    throw std::runtime_error("Handle was invalid");
  }

  SQLHDBC dbc = handle->dbc;

  return dbc;
}

} // namespace

using namespace rdb2;

//' Open database connection
//'
//' Open connection to database
//'
//' @param conn_string ODBC driver connection string to database
//'
//' @return connection handle
//'
//' @export
// [[Rcpp::export]]
SEXP dbGetConn(const std::string& conn_string) {

  SQLHDBC dbc = NULL;
  dbc = rdb2::getConn(conn_string);

  return get_R_handle_from_SQL_handle(dbc);
}

//' @export
// [[Rcpp::export(name=".dbCloseConnInternal")]]
void dbCloseConnInternal(SEXP& R_handle) {

  pODBCHandle handle = __get_handle_from_R_handle(R_handle);

  __close_handle(handle);

  R_ReleaseObject(R_handle);

}

//' Set login timeout when connecting to database
//'
//' This setting does not affect existing connections but
//' future connections in this session will use the modified value
//'
//' @param timeout timeout value in seconds
//'
//' @export
// [[Rcpp::export]]
void dbSetLoginTimeout(long timeout) {

  if (timeout > 0) {
    rdb2::login_timeout = timeout;
  }

}

//' Set connection timeout when connecting to database
//'
//' This setting does not affect existing connections but
//' future connections in this session will use the modified value
//'
//' @param timeout timeout value in seconds
//'
//' @export
// [[Rcpp::export]]
void dbSetConnectionTimeout(long timeout) {

  if (timeout > 0) {
    rdb2::connection_timeout = timeout;
  }
}

//' Set default chunk size to use when writing to database
//'
//' This setting is expressed as number of rows
//'
//' @param chunk_size chunk size in rows
//'
//' @export
// [[Rcpp::export]]
void dbSetWriteChunkSize(unsigned int chunk_size) {
  	
  write_chunk_size = chunk_size;
}

//' Set default chunk size to use when reading from database
//'
//' This setting is expressed as number of rows
//'
//' @param chunk_size chunk size in rows
//'
//' @export
// [[Rcpp::export]]
void dbSetReadChunkSize(unsigned int chunk_size) {

  read_chunk_size = chunk_size;
}

//' Get current default chunk size used when writing to database
//'
//' This setting is expressed as number of rows
//'
//' @return chunk_size chunk size in rows
//'
//' @export
// [[Rcpp::export]]
unsigned int dbGetWriteChunkSize() {
  return write_chunk_size;
}

//' Get current default chunk size used when reading from database
//'
//' This setting is expressed as number of rows
//'
//' @return chunk_size chunk size in rows
//'
//' @export
// [[Rcpp::export]]
unsigned int dbGetReadChunkSize() {
  return read_chunk_size;
}

//' @export
// [[Rcpp::export(name=".is_null_externalptr")]]
bool is_null_externalptr(const SEXP& R_handle) {
  SEXP ptr = Rf_getAttrib(R_handle, Rf_install("handle_ptr"));

  return (!R_ExternalPtrAddr(ptr));
}
