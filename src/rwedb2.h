
/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2.h
 *
 *  Created on: Mar 17, 2017
 *      Author: karthik
 */

#ifndef SRC_RWEDB2_H_
#define SRC_RWEDB2_H_

#include "rwedb2_DML.h"
#include "rwedb2_utils.h"

namespace rdb2 {

struct odbc_env_handle {
	SQLHENV env;
	int conn_count;  // number of dbc connections under this environment
	
	odbc_env_handle() {
		env = NULL;
		conn_count = 0;
	}

};

struct odbc_stmt_handle {
  SQLHSTMT stmt;
  odbc_stmt_handle(SQLHSTMT statement = NULL) {
    stmt = statement;
  }
  ~odbc_stmt_handle() {
    if (stmt != NULL) {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
  }
};

void closeConn(SQLHDBC dbc, bool disconnect = true);

SQLHDBC getConn(const std::string& conn_string, const long& login_timeout = 120, 
                  const long& connection_timeout = 120);
                  
void execute_update(const SQLHDBC& dbc, const std::string& query);

void setInterruptHandler(void (*fn) (void));

void clearInterruptHandler();

void checkInterrupt();

} // namespace
#endif /* SRC_RWEDB2_H_ */
