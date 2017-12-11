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

context("Test chunk size")

test_that('Check setting write chunk size', {
    dbSetWriteChunkSize(123456)
    expect_equal(dbGetWriteChunkSize(), 123456)
  })

test_that('Check setting read chunk size', {
    dbSetReadChunkSize(100)
    expect_equal(dbGetReadChunkSize(), 100)
  })


