#  Licensed Materials - Property of IBM
#  
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 

.onLoad <- function (libname = find.package("RDB2"), pkgname = "RDB2") {
  Sys.setlocale(category = "LC_ALL", locale = "C")
}

.onUnload <- function (libpath) {
	library.dynam.unload("RDB2", libpath)
}