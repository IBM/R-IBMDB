#  Licensed Materials - Property of IBM
#  
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 
#' Write dataframe to table in specified DB
#' 
#' @param df dataframe 
#' @param handle database connection handle
#' @param tbl_name Name of table to create. If table is a temporary table, use prefix SESSION. Eg. SESSION.MY_TABLE instead of MY_TABLE
#' @param col_names vector with list of valid column names for the table. If null, column names from the dataframe will be used.
#' @param db_write_coltypes vector containing valid DB2 SQL types for the columns to be created. If null, column types will be inferred from the dataframe
#' @param create_table Boolean to specify whether to create table in database or write to existing table
#' @param temp Boolean to specify whether created table is temporary or regular table. Unless db_name is a workspace db, temp must be TRUE
#' @param quick Boolean to specify if table should be created to write without logging - can help speed up writes. default is TRUE
#' @param chunk_size Specify number of rows to write at a time. Larger chunksize = faster writes but too large can cause memory errors and DB log buffer overflows.
#' @param verbose Prints SQL query that is being executed 
#'
#' @return None
#'
#' @export

dbWriteTable <- function(df, handle, tbl_name, col_names = NULL, db_write_coltypes = NULL, create_table = FALSE, temp = FALSE, 
		quick = TRUE, chunk_size = NULL, verbose = FALSE) {
	
	if (!rdb2.check_handle(handle)) {
		message("handle is not a valid RDB2 handle")
		return (NULL)
	}
	
	if (!is.null(col_names) && (length(col_names) != length(df))) {
		message("Number of column names provided does not match number of columns in dataframe")
		return (NULL)
	}
	
	if (!is.null(db_write_coltypes) && (length(db_write_coltypes) != length(df))) {
		message("Number of SQL column types provided does not match number of columns in dataframe")
		return (NULL)
	}
	
	if(nrow(df) == 0) {
		message("Dataframe is empty (number of rows = 0). Nothing to write.")
		return (NULL)
	}
	
	# check that chunk size is valid
	if (is.null(chunk_size)) {
		chunk_size = as.integer(dbGetWriteChunkSize())
	}
	
	if (chunk_size <= 0) {
		message(paste("chunk_size must be positive. Please try again"))
		return (NULL)
	}
	
	# convert logical, Date and factor columns to character
	cols <- which(sapply(df, function(x) is.factor(x) || is.logical(x) || class(x) == "Date"))
	if (length(cols) > 0) {
		df[cols] <- lapply(df[cols], as.character)
	}
	
	R_coltypes <- sapply(df, class)
	
	if (is.null(col_names)) {
		col_names <- colnames(df)
	}
	
	if (create_table) {
		if(!is.null(db_write_coltypes)) {
			db_write_coltypes <- sapply(db_write_coltypes, function(x) toupper(x))
		} else {
			db_write_coltypes <- infer_SQL_coltypes(df)
		}
		
		RDB2::dbCreateTable(handle, tbl_name, col_names, db_write_coltypes, temp, quick, verbose)
	}
	
	num_rows = nrow(df)
	
	col_lengths <- rdb2.calc_max_varchar(df)
	
	sapply (seq(1, nrow(df), chunk_size), 
			function(x) rdb2.insert_chunk(handle, df, x, chunk_size, 
						tbl_name, col_names, R_coltypes, col_lengths, verbose))
	
	invisible()
}

#' Read table from DB into an R dataframe
#' 
#' @param handle database connection handle
#' @param db_tblname Name of table to create. If table is a temporary table, use prefix SESSION. Eg. SESSION.MY_TABLE instead of MY_TABLE
#' @param db_colnames vector with list of valid column names to read from the table. Default is all columns.
#' @param num_rows number of rows to read. Default is all rows.
#' @param where_clause valid SQL where clause to filter the rows. NOTE This is meant for simple filters without any JOIN elements. For complex filters including joins, subqueries etc. use dbExecuteUpdate
#' @param order_clause vector of column names to use for ORDER BY. 
#' @param chunk_size Specify number of rows to read at a time. Larger chunksize = faster reads but too large can cause memory errors.
#' @param verbose Prints SQL query that is being executed 
#' @param stringsAsFactors logical should character vectors be converted to factors? Defaults to result of getOption("stringsAsFactors") 
#' 
#' @return DataFrame containing contents of specified table
#'
#' @export

dbReadTable <- function(handle, db_tblname, db_colnames = c('*'), num_rows = NULL, where_clause = NULL, 
		order_clause = NULL, chunk_size = NULL, verbose = FALSE, stringsAsFactors = NULL) {
	
  if (!rdb2.check_handle(handle)) {
      message("handle is not a valid RDB2 handle")
      return (NULL)
    }
    
  if(is.null(stringsAsFactors)) {
		stringsAsFactors = getOption("stringsAsFactors")
	}
	
	if (!is.logical(stringsAsFactors) || is.na(stringsAsFactors)) {
		message(paste("stringsAsFactors must be TRUE or FALSE, not"), stringsAsFactors)
		return (NULL)
	}
	
	# check that chunk size is valid
	if (is.null(chunk_size)) {
		chunk_size = as.integer(dbGetReadChunkSize())
	}
	
	if (chunk_size <= 0) {
		message(paste("chunk_size must be positive. Please try again"))
		return (NULL)
	}
		
	db_colnames <- lapply(db_colnames, function(x) trimws(x))  # strip out whitespace from column names
	if (is.element('*', db_colnames)) {
		db_colnames <- c('*')
	}
	
	readQuery <- paste("SELECT", paste(db_colnames, collapse = ", "), "FROM", db_tblname)
	if (!is.null(where_clause)) {
		readQuery <- paste(readQuery, 'WHERE', where_clause)
	}
	
	if (!is.null(order_clause)) {
		readQuery <- paste(readQuery, 'ORDER BY', paste(order_clause, collapse = ','))
	}
	
  # see below link which recommends using OPTIMIZE along with FETCH for DB2 LUW
  # https://www.ibm.com/support/knowledgecenter/SSEPGG_10.1.0/com.ibm.db2.luw.admin.perf.doc/doc/c0055223.html
	if(!is.null(num_rows) && num_rows > 0) {
		readQuery <- paste(readQuery, "FETCH FIRST", as.integer(num_rows), 
                  "ROWS ONLY OPTIMIZE FOR ", as.integer(num_rows), "ROWS")
	}
	
	if (verbose) {
		print (readQuery)
	}

	df <- NA
	df <- RDB2::.dbExecuteQueryInternal(handle, readQuery, chunk_size, stringsAsFactors) 
	
	return (df)
}


#' Create table in specified DB
#' 
#' @param handle database connection handle
#' @param tbl_name Name of table to create. If table is a temporary table, use prefix SESSION. Eg. SESSION.MY_TABLE instead of MY_TABLE
#' @param db_write_colnames vector with list of valid column names for the table
#' @param db_write_coltypes vector containing valid DB2 SQL types for the columns to be created.
#' @param temp Boolean to specify whether created table is temporary or regular table. temp must be TRUE except for workspace DBs
#' @param quick Boolean to specify if table should be created to write without logging - can help speed up writes. default is TRUE
#' @param verbose Prints SQL query that is being executed 
#'
#' @return None
#'
#' @export

dbCreateTable <- function(handle, tbl_name, db_write_colnames, db_write_coltypes, temp = FALSE, quick = TRUE
					, verbose = FALSE) {
	
	if (!rdb2.check_handle(handle)) {
		message("handle is not a valid RDB2 handle")
		return (NULL)
	}
	
	if (temp && substr(toupper(tbl_name), 1, 8) != 'SESSION.') {
		message('ERROR: Temp table names must be prefixed by "SESSION.". Eg. SESSION.MYTABLE')
		return (NULL)
	}
	
  if (!temp && substr(toupper(tbl_name), 1, 8) == 'SESSION.') {
    message('ERROR: Only temporary table names must be prefixed by "SESSION.". Eg. SESSION.MYTABLE. Set temp parameter to TRUE')
    return (NULL)
  }
  
  if (is.null(db_write_coltypes)) {
    warning ("SQL column types must be specified when creating table.")
    return (NULL)
  }
  
  if (length(db_write_coltypes) != length(db_write_colnames)) {
    warning (paste("SQL column types must be specified for each column when creating table." ,
      "Length of db_write_colname parameter does not match length of db_write_coltypes"))
    return (NULL)
  }
  
	db_col_string <- paste(sapply(seq(1:length(db_write_colnames)), function(x) paste(db_write_colnames[x], db_write_coltypes[x])), collapse = ", ")
	
	if (temp) {
		createSQL <- paste('DECLARE GLOBAL TEMPORARY TABLE', tbl_name, '(', db_col_string,')', 'ON COMMIT PRESERVE ROWS NOT LOGGED')
	} else {
		createSQL <- paste('CREATE TABLE', tbl_name, '(', db_col_string,')')
		if (quick)
			createSQL <- paste(createSQL, ' ORGANIZE BY ROW NOT LOGGED INITIALLY')
	}
	
	if (verbose) {
		print (createSQL)
	}
	
	RDB2::dbExecuteUpdate(handle, createSQL)
	invisible()
}

#' Delete table in specified DB
#' 
#' @param handle database connection handle
#' @param tbl_name table name to drop. If table is a temporary table, use prefix SESSION. Eg. SESSION.MY_TABLE instead of MY_TABLE
#' @param verbose Prints SQL query that is being executed 
#'
#' @return None
#'
#' @export

dbDropTable <- function(handle, tbl_name, verbose = FALSE) {
	
	if (!rdb2.check_handle(handle)) {
		message("handle is not a valid RDB2 handle")
		return (NULL)
	}
	
	dropSQL <- paste('DROP TABLE', tbl_name)
	
	if (verbose) {
		print (dropSQL)
	}
	
	RDB2::dbExecuteUpdate(handle, dropSQL)
	invisible()
}

#' Execute provided SQL query on the given DB and return results
#' 
#' This function is meant to be used for more complex select queries
#' involving joins, subqueries etc. 
#' 
#' @param handle database connection handle
#' @param query Valid SQL query that will be executed
#' @param chunk_size Number of rows to read at a time. Larger chunksize = faster reads but higher
#' risk of running into memory issues. Default is 1
#' @param stringsAsFactors logical should character vectors be converted to factors? Defaults to result of getOption("stringsAsFactors") 
#' @return DataFrame containing results from executing specified query
#'
#' @export

dbExecuteQuery <- function(handle, query, chunk_size = NULL, stringsAsFactors = NULL) {
	
  if (!rdb2.check_handle(handle)) {
    message("handle is not a valid RDB2 handle")
    return (NULL)
  }
  
  if(is.null(stringsAsFactors)) {
		stringsAsFactors = getOption("stringsAsFactors")
	}
	
	if (!is.logical(stringsAsFactors) || is.na(stringsAsFactors)) {
		message(paste("stringsAsFactors must be TRUE or FALSE, not"), stringsAsFactors)
		return (NULL)
	}
	
	# check that chunk size is valid
	if (is.null(chunk_size)) {
		chunk_size = as.integer(dbGetReadChunkSize())
	}
	
	if (chunk_size <= 0) {
		message(paste("chunk_size must be positive. Please try again"))
		return (NULL)
	}
	
	RDB2::.dbExecuteQueryInternal(handle, query, chunk_size, stringsAsFactors)
}

#' Close database connection
#'
#' close connection to database if a connection is open
#'
#' @param handle connection handle to database
#'
#' @return None
#'
#' @export
  
dbCloseConn <- function(handle) {
  if (!rdb2.check_handle(handle)) {
    message("handle is not a valid RDB2 handle")
    return (NULL)
  }
  
  RDB2::.dbCloseConnInternal(handle)
}
			

