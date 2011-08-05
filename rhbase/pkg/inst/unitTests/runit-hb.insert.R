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
checkTrue(hb.insert("testtable",list( list(1,c("x","y","z"),list(1,1,1)))))
checkTrue(hb.insert("testtable",list( list(2,c("x","y","z"),list(1,1,1)),
                                      list(3,c("x:a","y:b","z:b"),list(1,1,1)),
                                      list(4,c("x:c","y:c"),list(1,1)),
                                      list(5,c("x:d"),list(1)))))
checkTrue({
  x <- hb.get("testtable",1)
  rs <- c()

  rs <- c(rs, x[[1]][[1]]==1, all(x[[1]][[2]]==c("x:","y:","z:")),all(unlist(x[[1]][[3]])==1))

  x <- hb.get("testtable",3,"x")
  rs <- c(rs, x[[1]][[1]]==3, all(x[[1]][[2]]==c("x:a")),all(unlist(x[[1]][[3]])==1))

  x <- hb.get("testtable",list(4,5),"x")

  rs <- c(rs,all(unlist(lapply(x,"[[",1))==c(4,5)), all(unlist(lapply(x,"[[",2))==c("x:c","x:d")))
  
  all(rs)
})
                                     
                                     
                                     
