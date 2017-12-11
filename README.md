# RDB2
RDB2 is a limited ODBC based interface between R and DB2. 
It offers much more efficient reads and writes from R dataframes to DB2 tables than generic packages such as RJDBC and RODBC, especially for large datasets

## Installation ##

Package installation steps are in install_notebook_library.sh in the RWE-Notebooks repo

The RDB2.zip file is created in Jenkins job

```
WHC-RWE-Dev/RWE_Notebooks_libraries_Nightly
```


### Usage Instructions

For R:-
```
library("RDB2")
```

### Development Instructions

For development, the RDB2 library comes with a test suite that uses the testthat R package.

To use it, ensure that a database called PUBWRKSP exists on the test server.

Read the comments in ```rdb2_test_setup.sh``` from the RDB2 tests folder and run it.

Run the following code after installing RDB2 and dataConn
```
chmod -R 777 /tmp/RDB2
su db2inst1

/usr/bin/Rscript -<< EOF
library("devtools")
setwd("/tmp")
devtools::check("/tmp/RDB2/")
EOF

```

#### Leak checking with valgrind
First install valgrind on the target system.

The file suppressions.txt in the tests folder suppresses some known non-fatal issues in R and the DB2 ODBC driver.

Run R with valgrind as debugger using the following command and the correct path for suppressions.txt.
```
 R -d "valgrind --track-origins=yes --leak-check=full --suppressions=suppressions.txt"
```

You can now use R as usual and quit() when done. Valgrind will display a report with the details of memory used and any potential leaks.