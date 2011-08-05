# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 
library(utils)
.hdfsEnv <- new.env()
.onLoad <- function(libname,pkgname){
  vrs <- packageDescription(pkgname, lib.loc = libname, fields = "Version",
                            drop = TRUE)
  if (Sys.getenv("HADOOP_HOME") == "") stop(sprintf("Environment variable HADOOP_HOME must be set before loading package %s", pkgname))
  if (Sys.getenv("HADOOP_CONF") == "") stop(sprintf("Environment variable HADOOP_CONF must be set before loading package %s", pkgname))
  packageStartupMessage("This is ", pkgname, " ", vrs, ". ",
                        "For overview type ", sQuote(paste("?", pkgname, sep="")), ".",  
                        "\nHADOOP_HOME=", Sys.getenv("HADOOP_HOME"),
                        "\nHADOOP_CONF=", Sys.getenv("HADOOP_CONF"))
  hdfs.init()
}
