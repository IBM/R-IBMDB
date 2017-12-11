#  Licensed Materials - Property of IBM
#  
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
# 

# This script needs to be run once only on development machines to populate the DB with test data
# for the automated tests that come with RDB2 and dataConn pkgs
# It needs to be run under the user id of a user that has connect, grant and write access to pubwrksp db
# simplest is to run this as db2inst1

db2 connect to pubwrksp

db2 'CREATE TABLE "RDB2_TEST_SCRIPT_DATA"  ( "DECIMAL" VARCHAR(32 OCTETS) , "DOUBLE" DOUBLE , "SMALLINT" SMALLINT , "BIGINT" BIGINT , "VARCHAR" VARCHAR(1024 OCTETS) , "CHAR" CHAR(1 OCTETS) , "TIMESTAMP" TIMESTAMP , "INTEGER" INTEGER , "DATE" DATE , "FLOAT" DOUBLE , "REAL" REAL ) IN "USERSPACE1" ORGANIZE BY COLUMN'

db2 "import from rdb2_test_script_data.csv of del allow write access messages dummy.msg insert into rdb2_test_script_data"

db2 "grant select on rdb2_test_script_data to public"

db2 'CREATE TABLE "UNICODE_TEST"  ( "LANG" VARCHAR(256 OCTETS) , "DATA" VARCHAR(512 OCTETS) ) IN "USERSPACE1" ORGANIZE BY COLUMN'

db2 "import from unicode.csv of del allow write access messages dummy.msg insert into UNICODE_TEST"

db2 "grant select on unicode_test to public"

db2 terminate
