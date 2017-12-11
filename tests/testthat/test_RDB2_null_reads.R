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

context("Test RDB2 reads NULL values correctly")

connString <- 'DSN=PUBWRKSP'
h <- dbGetConn(connString)
data_tbl_name <- 'db2inst1.RDB2_TEST_SCRIPT_DATA'


checkConnection <- function() {
    if (is.null(h) || .is_null_externalptr(h)) {
      skip ('No valid connection')
    }
}

test_that('read table reads NULL values in VARCHAR column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT VARCHAR FROM ', data_tbl_name,'WHERE VARCHAR IS NULL'))
    expect_equal(nrow(n), 189)
  })

test_that('read table reads NULL values in DOUBLE column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT DOUBLE FROM ', data_tbl_name,'WHERE DOUBLE IS NULL'))
    expect_equal(nrow(n), 3)
  })

test_that('read table reads NULL values in SMALLINT column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT SMALLINT FROM ', data_tbl_name,'WHERE SMALLINT IS NULL'))
    expect_equal(nrow(n), 4)
  })

test_that('read table reads NULL values in BIGINT column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT BIGINT FROM ', data_tbl_name,'WHERE BIGINT IS NULL'))
    expect_equal(nrow(n), 2)
  })

test_that('read table reads NULL values in TIMESTAMP column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT TIMESTAMP FROM ', data_tbl_name,'WHERE TIMESTAMP IS NULL'))
    expect_equal(nrow(n), 11)
  })

test_that('read table reads NULL values in DATE column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT DATE FROM ', data_tbl_name,'WHERE DATE IS NULL'))
    expect_equal(nrow(n), 12)
  })

test_that('read table reads NULL values in REAL column correctly', {
    checkConnection()
    n <- dbExecuteQuery(h, paste('SELECT REAL FROM ', data_tbl_name,'WHERE REAL IS NULL'))
    expect_equal(nrow(n), 3)
  })

# close connection
dbCloseConn(h)
