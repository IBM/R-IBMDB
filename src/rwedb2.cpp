
/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2.cpp
 *
 *  Created on: Mar 17, 2017
 *      Author: karthik
 */

#include "rwedb2.h"


namespace rdb2 {

struct odbc_env_handle env_struct;

// function pointer to interrupt handler
// the wrapper code that uses this library needs to define this in a way
// that catches C++ exceptions and handles it or passes it through in the appropriate
// way to the language in which the calling code is written

static void (*interrupt_fn) (void);

void setInterruptHandler(void (*fn) (void)) {
  interrupt_fn = fn;
}

void clearInterruptHandler() {
  interrupt_fn = NULL;
}

void checkInterrupt() {
  if (interrupt_fn != NULL) {
    interrupt_fn();
  }
}

void closeConn(SQLHDBC dbc, bool disconnect) {
  SQLRETURN ret;

  /* disconnect */
  if (dbc == NULL)
    return;

  if (disconnect) {   /* disconnect from driver */
    if (!SQL_SUCCEEDED(ret = SQLDisconnect(dbc))) {
      throw std::runtime_error(extract_error("Error while disconnecting from database", dbc, SQL_HANDLE_DBC));
    }
  }
   
  if (!SQL_SUCCEEDED(ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc))) {
    throw std::runtime_error(extract_error("Error while freeing database handle", dbc, SQL_HANDLE_DBC));
  }

  env_struct.conn_count--;
  
  if (env_struct.conn_count == 0) {
    if (!SQL_SUCCEEDED(ret = SQLFreeHandle(SQL_HANDLE_ENV, env_struct.env))) {
      throw std::runtime_error(extract_error("Error while freeing environment handle", env_struct.env, SQL_HANDLE_ENV));
    }
    env_struct.env = NULL;
  }

}

SQLHDBC getConn(const std::string& conn_string, const long& login_timeout, const long& connection_timeout) {

  SQLHDBC dbc = NULL;
  SQLRETURN ret; /* ODBC API return status */
  std::string err_msg;

  if (env_struct.env == NULL) {
    /* Allocate an environment handle */
    if (!SQL_SUCCEEDED(ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_struct.env))) {
      throw std::runtime_error(extract_error("Error while allocating env handle", env_struct.env, SQL_HANDLE_ENV));
    }

    /* We want ODBC 3 support */
    ret = SQLSetEnvAttr(env_struct.env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error(
          extract_error("Error when setting ODBC 3 support in " + std::string(__func__), env_struct.env, SQL_HANDLE_ENV));
    }
  }


  /* Allocate a connection handle */
  if (!SQL_SUCCEEDED(ret = SQLAllocHandle(SQL_HANDLE_DBC, env_struct.env, &dbc))) {
    throw std::runtime_error(extract_error("Error while allocating dbc handle", env_struct.env, SQL_HANDLE_ENV));
  }
  env_struct.conn_count++;

  ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) login_timeout, 0);
  if (!SQL_SUCCEEDED(ret)) {
    err_msg = extract_error("Failed to set login timeout", dbc, SQL_HANDLE_DBC);
    closeConn(dbc, false);
    throw std::runtime_error(err_msg);
  }

  ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER) connection_timeout, 0);
  if (!SQL_SUCCEEDED(ret)) {
    err_msg = extract_error("Failed to set connection timeout", dbc, SQL_HANDLE_DBC);
    closeConn(dbc, false);
    throw std::runtime_error(err_msg);
  }

  /* Connect to the DSN */
  ret = SQLDriverConnectW(dbc, NULL, get_UTF16_string(conn_string).get(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

  if (!SQL_SUCCEEDED(ret)) {
    err_msg = extract_error("Failed to connect", dbc, SQL_HANDLE_DBC);
    closeConn(dbc, false);
    throw std::runtime_error(err_msg);
  }

  return dbc;

}

void execute_update(const SQLHDBC& dbc, const std::string& query) {
  struct odbc_stmt_handle stmt_holder;
  SQLRETURN ret; /* ODBC API return status */

  // set autocommit to true
  if (!SQL_SUCCEEDED(ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) (long) SQL_AUTOCOMMIT_ON, 0))) {
    throw std::runtime_error(extract_error("Error modifying autocommit parameter", dbc, SQL_HANDLE_DBC));
  }

  /* Allocate a statement handle */
  if (!SQL_SUCCEEDED(ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_holder.stmt))) {
    throw std::runtime_error(
        extract_error("Error in " + std::string(__func__) + " while allocating statement", dbc, SQL_HANDLE_DBC).c_str());
  }

  if (!SQL_SUCCEEDED(ret = SQLExecDirectW(stmt_holder.stmt, get_UTF16_string(query).get(), SQL_NTS))) {
    throw std::runtime_error(extract_error("Error while executing query", stmt_holder.stmt, SQL_HANDLE_STMT).c_str());
  }
}

}  // namespace
