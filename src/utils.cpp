/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2016, 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
#include "utils.h"
#include <Rcpp.h>

namespace rdb2 {

void R_msg(const std::string& txt) {
  Rcpp::Function msg("message");

  msg(txt);
}
}
