/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#ifndef RDB2_CONNECTIONS_H
#define RDB2_CONNECTIONS_H

#include <string>
#include <Rcpp.h>

namespace rdb2 {
SQLHDBC get_dbc_handle(const SEXP& handle);

SEXP get_R_handle_from_SQL_handle(const SQLHDBC& dbc);
}
#endif

