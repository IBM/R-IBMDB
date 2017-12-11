
/*
 Licensed Materials - Property of IBM
 
 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2_utils.h
 *
 *  Created on: Mar 17, 2017
 *      Author: karthik
 */

#ifndef SRC_RWEDB2_UTILS_H_
#define SRC_RWEDB2_UTILS_H_

#include <sql.h>
#include <sqlext.h>
#include <memory>


#define SQL_DECFLOAT -360  // this does not seem to be defined in the unixODBC headers

namespace rdb2 {
std::string extract_error(const std::string& fn, SQLHANDLE handle, SQLSMALLINT type);

void encodeUTF8StringAsUTF16(SQLWCHAR* const dest, const std::string& source);

std::string encodeUTF16StringAsUTF8(const SQLWCHAR* const source, const SQLLEN& len);

std::shared_ptr<SQLWCHAR> get_UTF16_string(const std::string& source);

#ifdef RDB2_DEBUG
void hexdump(void *mem, unsigned int len, unsigned int HEXDUMP_COLS);
#endif
}

#endif /* SRC_RWEDB2_UTILS_H_ */
