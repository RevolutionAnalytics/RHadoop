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

 
hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.new.table("scan.table","x","y"))
checkTrue(hb.insert("scan.table",list(
                                      list(1,c("x","y"),list(-1,2)),
                                      list(2,c("x","y"),list(-2,3)),
                                      list(3,c("x","y"),list(-3,4)),
                                      list(4,c("x"),list(-4)),
                                      list(5,c("x"),list(-6)),
                                      list(6,c("x"),list(-7)))))
                                      
sc <- hb.scan("scan.table",start=1,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))

rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,6)))

sc <- hb.scan("scan.table",start=1,end=6,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,5)))


sc <- hb.scan("scan.table",start=1,end=6,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,5)))


sc <- hb.scan("scan.table",start=1,colspec=c("y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,3)))


sc <- hb.scan("scan.table",start=NULL,colspec=c("y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(is.null(rsx))

checkTrue(hb.delete.table("scan.table"))
