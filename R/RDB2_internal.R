#  Licensed Materials - Property of IBM
#  
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 
# Internal functions used by RDB2 interface
# Author: karthik.jayaraman1@ibm.com

# Helper function to check if handle contains an externalptr
rdb2.check_handle <- function(handle) {
  ptr <- attr(handle, "handle_ptr")
  if (class(ptr) != "externalptr") {
    return (FALSE)
  } 
  
  return (!RDB2::.is_null_externalptr(handle))	
}

rdb2.SQL_mapping = list(character = 'VARCHAR', logical = 'VARCHAR', numeric = 'DOUBLE',
    integer = 'BIGINT', Date = 'VARCHAR', factor = 'VARCHAR')
  
rdb2.insert_chunk <- function(handle, df, idx, chunkSize, tbl_name, col_names, R_coltypes, 
		varchar_col_lengths, verbose) {

  lower = idx
  upper = min(idx + chunkSize - 1, nrow(df))
  
  chunk <- df[lower:upper, ]
  # R changes data type when subsetting from a dataframe with one column 
  # so let's change it back
  if (ncol(df) == 1) {
	  chunk <- as.data.frame(chunk)
	  names(chunk) <- names(df)
  }
  RDB2::.dbWriteTableInternal(handle, chunk, tbl_name, col_names, R_coltypes, varchar_col_lengths, verbose)
  
  invisible()
}

rdb2.infer_SQL_length <- function(df, R_class, colnum) {
	
	if (R_class == "numeric" || R_class == "integer") {
		return ("")
	}
	
	if (R_class == "logical") {
		return ("(5)")
	}
	
	if (R_class == "Date") {
		return ("(31)")
	}
	
	# get longest string in df[, colnum]. returns 1 if every element is NA or empty string
	maxlen = ifelse(!all(is.na(df[,colnum])), max(sapply(df[, colnum], function(x) nchar(as.character(x))), na.rm = TRUE), 1)
	maxlen = max(maxlen, 1)
	return (paste0("(", maxlen, ")"))
}

rdb2.get_length <- function(df, R_class, colnum) {
	if (R_class != "character") {
		return (0)
	}
	
	# get longest string in df[, colnum]. returns 1 if every element is NA
	maxlen = ifelse(!all(is.na(df[,colnum])), max(sapply(df[, colnum], function(x) nchar(x)), na.rm = TRUE), 1)
	return (maxlen)
}

rdb2.calc_max_varchar <- function(df) {
	# calculate maximum string length in each character column
	# This is used internally to optimize a DB2 ODBC requirement that stores 
	# character arrays in a somewhat wasteful way
	
	# df: dataframe for which we want to calculate column lengths
	
	ncols = ncol(df)
	classes <- sapply(df, class)
	
	col_lengths <- sapply(seq(ncols), function(x) rdb2.get_length(df, classes[[x]], x))
	
	return (col_lengths)
}