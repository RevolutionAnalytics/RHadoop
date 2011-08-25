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

test.hdfs.get <- function()
{
	x <- hdfs.defaults()
	y <- tempdir()
	## Load airline test data and put into Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="rhdfs")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	checkTrue(hdfs.get("/test/airline/AirlineDemo1kNoMissing.csv",y))
	checkTrue("AirlineDemo1kNoMissing.csv" %in% list.files(y))
	unlink(y)

	## Breakdown
	hdfs.delete("/test")
}
