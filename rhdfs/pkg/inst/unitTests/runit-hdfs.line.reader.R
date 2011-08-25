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

 
stopifnot(require(rhdfs, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.line.reader <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="rhdfs")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Test checks
	m <- hdfs.line.reader("/test/airline/AirlineDemo1kNoMissing.csv", n=100)
	x <- m$read()
	checkTrue(x[3] == "-3,18.583334,\"Monday\"")
        checkTrue(x[50] == "-7,12.166667,\"Monday\"")
	checkTrue(x[99] == "0,18.750002,\"Monday\"")
	m$close()
	
	##Breakdown
	rm(x)
        hdfs.delete("/test")
}

