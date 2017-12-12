#!/bin/sh

#  Licensed Materials - Property of IBM
#  
#  License: BSD 3-Clause
#
#  5747-C31, 5747-C32
# 
#  © Copyright IBM Corp. 2016, 2017    All Rights Reserved
# 
#  US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

set -ex
exec &> /opt/ibm/rwe/logs/install_notebook_libraries.log

# install RDB2 package
rm -rf /tmp/RDB2
mkdir -p /tmp/RDB2
unzip -d /tmp/RDB2 RDB2.zip
/usr/bin/Rscript -<< EOF
library("devtools")
library("Rcpp")
setwd("/tmp")
try(remove.packages("RDB2"), TRUE)
compileAttributes("RDB2")
install("RDB2")
EOF

# create RDB2 shared object
rm -rf /usr/include/rwedb2/*
rm -rf /usr/lib64/librwedb2*

mkdir -p /usr/include/rwedb2

cd /tmp/RDB2/src

g++ -m64 -std=c++11 -shared -Wl,-soname,librwedb2.so.1 -o librwedb2.so.1.0   rwedb2*.o

mv librwedb2.so.1.0 /usr/lib64/

ln -sf /usr/lib64/librwedb2.so.1.0 /usr/lib64/librwedb2.so

ln -sf /usr/lib64/librwedb2.so.1.0 /usr/lib64/librwedb2.so.1

chmod 0755 /usr/lib64/librwedb2*

ldconfig # this updates the OS cache to make it aware of the new shared library

# set up include files

cp /tmp/RDB2/src/utf8.h /usr/include/rwedb2/

cp -r /tmp/RDB2/src/utf8 /usr/include/rwedb2/

cp /tmp/RDB2/src/rwedb2*.h /usr/include/rwedb2/

chmod -R 755 /usr/include/rwedb2

