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

context("Test RDB2 Unicode capability")

connString <- 'DSN=PUBWRKSP'
h <- NULL
data_tbl_name <- 'db2inst1.UNICODE_TEST'
test_tbl_name <- 'DC_TEST_TABLE'

# variables to store cached dataframe for data from DB table

df_unicodedata <- NULL

load_df <- function(stringsAsFactors) {
    
  if (is.null(df_unicodedata)) {
    df_unicodedata <<- dbReadTable(h, data_tbl_name, c('*'), stringsAsFactors = FALSE)
  }
  
  NULL
}

dropIfExists <- function(h, tbl) {
  tbl_check <- dbExecuteQuery(h, 
    paste0("select count(tabname) from syscat.tables where tabname = '", tbl, "'"))
  if (tbl_check[[1]] > 0)
    dbDropTable(h, tbl)  
}

checkConnection <- function() {
    if (is.null(h) || .is_null_externalptr(h)) {
      skip ('No valid connection')
    }
}

h <- dbGetConn(connString)

test_that('Check that dataframe that was written and read back matches the original dataframe', {
    checkConnection()
    dropIfExists(h, test_tbl_name)
    load_df(FALSE)
    
    dbWriteTable(df_unicodedata, h, test_tbl_name, names(df_unicodedata), create_table = TRUE)
    result <- dbReadTable(h, test_tbl_name, stringsAsFactors = FALSE)
    
    temp2 <- df_unicodedata[do.call(order, as.list(df_unicodedata)),] 
    temp3 <- result[do.call(order, as.list(result)), ]
    mismatches <- which(temp2 != temp3)  
    expect_equal(length(mismatches), 0)
  })


# close connection to clean up
dbCloseConn(h)
