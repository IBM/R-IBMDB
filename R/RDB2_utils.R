#  Licensed Materials - Property of IBM
#  
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 
# Utility functions
# Author: karthik.jayaraman1@ibm.com


infer_SQL_coltypes <- function(df) {
 # infer SQL column types based on the dataframe column classes
 # df: dataframe for which we want to infer columnn types
	
  ncols = ncol(df)
  classes <- sapply(df, class)
  typenames <- sapply(classes, function(x) rdb2.SQL_mapping[[x]])
  
  col_lengths <- sapply(seq(ncols), function(x) rdb2.infer_SQL_length(df, classes[[x]], x))

  # create list of column types as VARCHAR(20), BIGINT etc.
  typenames <- sapply(seq(ncols), function(x) paste0(typenames[[x]], col_lengths[[x]]))
  return (typenames)
}
