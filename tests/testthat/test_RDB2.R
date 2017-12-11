#  Licensed Materials - Property of IBM
#  
#  License: BSD 3-Clause
#
# 5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 
suppressMessages(library(RDB2))

context("Test RDB2")

connString <- 'DSN=PUBWRKSP'
h <- NULL
data_tbl_name <- 'db2inst1.RDB2_TEST_SCRIPT_DATA'
test_tbl_name <- 'DC_TEST_TABLE'
test_tbl_schema <- toupper(Sys.info()[["user"]])

# variables to store cached dataframes for data from DB table
# there are two variables for dataframes based on whether stringsAsFactors is TRUE or FALSE

df_false_stringsAsFactors <- NULL
df_true_stringsAsFactors <- NULL

load_df <- function(stringsAsFactors) {
  
  if (stringsAsFactors && is.null(df_true_stringsAsFactors)) {
    df_true_stringsAsFactors <<- dbReadTable(h, data_tbl_name, c('*'), stringsAsFactors = TRUE)
  }
  
  if (!stringsAsFactors && is.null(df_false_stringsAsFactors)) {
    df_false_stringsAsFactors <<- dbReadTable(h, data_tbl_name, c('*'), stringsAsFactors = FALSE)
  }
  
  NULL
}

dropIfExists <- function(h, tbl) {
  tbl_check <- dbExecuteQuery(h, 
    paste0("select count(tabname) from syscat.tables where tabname = '", tbl, "' and tabschema = '", test_tbl_schema, "'"))
  if (tbl_check[[1]] > 0)
    dbDropTable(h, tbl)  
}

checkConnection <- function() {
    if (is.null(h) || .is_null_externalptr(h)) {
      skip ('No valid connection')
    }
}

test_that("close connection with NULL handle", {
    h <<- NULL
    expect_message(dbCloseConn(h), "handle is not a valid RDB2 handle")
  })

test_that("close connection with invalid handle", {
    h <<- 'fdgdf'
    expect_message(dbCloseConn(h), "handle is not a valid RDB2 handle")
  })

test_that("open/close connection", {
    h <<- dbGetConn(connString)
    expect_equal(class(attributes(h)$handle_ptr), "externalptr")
    expect_null(dbCloseConn(h))
  })


test_that("check close connection twice", {
    h <<- dbGetConn(connString)
    expect_null(dbCloseConn(h))
    expect_null(dbCloseConn(h))
  })

# now make a connection that we will use for the rest of the tests
h <- dbGetConn(connString)

test_that('read table returns correct types with stringsAsFactors true', {
    checkConnection()
    load_df(TRUE)
    classes <- paste(sapply(df_true_stringsAsFactors, class), collapse = ', ')
    expected_result <- 'factor, numeric, integer, numeric, factor, factor, factor, integer, factor, numeric, numeric'
    expect_equal(classes, expected_result)
  })

test_that('read table returns correct types with stringsAsFactors false', {
    checkConnection()
    load_df(FALSE)
    classes <- paste(sapply(df_false_stringsAsFactors, class), collapse = ', ')
    expected_result <- 'character, numeric, integer, numeric, character, character, character, integer, character, numeric, numeric'
    expect_equal(classes, expected_result)
  })

test_that('executeQuery returns correct types with stringsAsFactors false', {
    checkConnection()
    load_df(FALSE)
    classes <- paste(sapply(df_false_stringsAsFactors, class), collapse = ', ')
    expected_result <- 'character, numeric, integer, numeric, character, character, character, integer, character, numeric, numeric'
    expect_equal(classes, expected_result)
  })

test_that('executeQuery returns correct types with stringsAsFactors true', {
    checkConnection()
    load_df(TRUE)
    classes <- paste(sapply(df_true_stringsAsFactors, class), collapse = ', ')
    expected_result <- 'factor, numeric, integer, numeric, factor, factor, factor, integer, factor, numeric, numeric'
    expect_equal(classes, expected_result)
  })

test_that('readTable and executeQuery return identical results', {
    checkConnection()
    load_df(TRUE)
    df2 <- dbExecuteQuery(h, paste('select * from ', data_tbl_name), stringsAsFactors = TRUE)
    temp2 <- df2[do.call(order, as.list(df2)),] 
    temp3 <- df_true_stringsAsFactors[do.call(order, as.list(df_true_stringsAsFactors)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Check that dataframe that was written and read back matches the original dataframe', {
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    dbWriteTable(df_false_stringsAsFactors, h, test_tbl_name, names(df_false_stringsAsFactors), create_table = TRUE)
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE)
    
    temp2 <- df_false_stringsAsFactors[do.call(order, as.list(df_false_stringsAsFactors)),] 
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Check writing to DATE SQL column', {
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 9], stringsAsFactors = FALSE)
    names(t) <- 'DATE'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('DATE'), 
      create_table = TRUE)
    result <- dbReadTable( h, test_tbl_name, stringsAsFactors = FALSE)
    temp2 <- t[do.call(order, as.list(t)),] 
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Check writing to TIMESTAMP SQL column', {
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 7], stringsAsFactors = FALSE)
    names(t) <- 'TIMESTAMP'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('TIMESTAMP'), 
      create_table = TRUE)
    
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE)
    temp2 <- t[do.call(order, as.list(t)),] 
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Test writing number represented as string to DECIMAL column', {
    # Note that this test assumes the data in the DECIMAL column has a maximum of 10 digits
    # after the decimal point. If there are more than that, it will show mismatches because of DB2 rounding
    # This is not a bug  - change the scale column in dbWriteTable below to match the number of digits 
    # required after the decimal point. eg. DECIMAL(25, 15) instead of DECIMAL (20, 10)
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 1], stringsAsFactors = FALSE)
    names(t) <- 'DECIMAL'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('DECIMAL(31,10)'), 
      create_table = TRUE)
    # we cast the result to numeric so that we are comparing the numerical value
    # to compare the string value, change the precision when writing table eg. DECIMAL(31, 10) instead of DECIMAL(20,5)
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE) 
    t[,1] <- as.numeric(t[,1])
    result[,1] <- as.numeric(result[,1])
    temp2 <- t[do.call(order, as.list(t)),]
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Test writing number represented as string to NUMERIC column', {
    # Note that this test assumes the data in the NUMERIC column has a maximum of 10 digits
    # after the NUMERIC point. If there are more than that, it will show mismatches because of DB2 rounding
    # This is not a bug  - change the scale column in dbWriteTable below to match the number of digits 
    # required after the NUMERIC point. eg. NUMERIC(25, 15) instead of NUMERIC (20, 10)
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 1], stringsAsFactors = FALSE)
    names(t) <- 'NUMERIC'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('NUMERIC(31,10)'), 
      create_table = TRUE)
    # we cast the result to numeric so that we are comparing the numerical value
    # to compare the string value, change the precision when writing table eg. NUMERIC(20, 10) instead of NUMERIC(20,5)
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE) 
    t[,1] <- as.numeric(t[,1])
    result[,1] <- as.numeric(result[,1])
    temp2 <- t[do.call(order, as.list(t)),]
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Test writing number represented as string to DECFLOAT column', {
    # Note that this test will fail if the string data has more precision than
    # DECFLOAT(34) can handle
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 1], stringsAsFactors = FALSE)
    names(t) <- 'DECFLOAT_34'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('DECFLOAT(34)'), 
      create_table = TRUE)
    # we cast the result to numeric so that we are comparing the numerical value
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE) 
    t[,1] <- as.numeric(t[,1])
    result[,1] <- as.numeric(result[,1])
    temp2 <- t[do.call(order, as.list(t)),]
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })

test_that('Test writing number represented as numeric to DECIMAL column', {
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    t <- as.data.frame(df_false_stringsAsFactors[, 2])
    names(t) <- 'DOUBLE'
    dbWriteTable(t, h, test_tbl_name, names(t), db_write_coltypes = c('DECIMAL(20,10)'), 
      create_table = TRUE)
    # we cast the result to numeric so that we are comparing the numerical value
    # to compare the string value, change the precision when writing table eg. DECIMAL(20, 10) instead of DECIMAL(20,5)
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE) 
    temp2 <- as.numeric(t[do.call(order, as.list(t)),])
    temp3 <- sort(as.numeric(result[,]), na.last = TRUE)
    mismatches <- which((temp2 - temp3) > 0.0001)
    expect_equal(length(mismatches), 0)
  })

test_that('Test create temporary table', {
    checkConnection()
    expect_null(dbCreateTable(h, paste0('SESSION.', test_tbl_name), c('col1'), c('BIGINT'), temp = TRUE))
  })

test_that('Test executeUpdate', {
    checkConnection()
    expect_null(dbExecuteUpdate(h, paste('DROP TABLE ', paste0('SESSION.', test_tbl_name))))
  })

test_that('Test reading and writing to temp table', {
    checkConnection()
    load_df(FALSE)
    dbWriteTable(df_false_stringsAsFactors, h, paste0('session.', test_tbl_name), names(df_false_stringsAsFactors), create_table = TRUE, temp = TRUE)
    result <- dbReadTable(h, paste0('session.', test_tbl_name), stringsAsFactors = FALSE)
    
    temp2 <- df_false_stringsAsFactors[do.call(order, as.list(df_false_stringsAsFactors)),] 
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  # This cell should produce output of 0
    expect_equal(length(mismatches), 0)
  })

# close connection to clean up
dbCloseConn(h)
