/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#include "dc.h"

using namespace rdb2;

//' Execute provided SQL on the given db
//' 
//' This function is meant to be used primarily for INSERT INTO .. SELECT 
//' on the DB and does no checking for SQL validity etc.
//' 
//' @param handle database connection handle
//' @param executeSQL Valid SQL query to execute on database
//'
//' @return None
//'
//' @export
// [[Rcpp::export]]

void dbExecuteUpdate(const SEXP& handle, const std::string& executeSQL) {

  SQLHDBC dbc = get_dbc_handle(handle);

  execute_update(dbc, executeSQL);
}
