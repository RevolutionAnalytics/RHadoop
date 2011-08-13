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
checkTrue(hb.insert("testtable",list(list(1001,c("x"),list(1)))))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  x[[1]][[3]]==1
})
checkTrue(hb.insert("testtable",list(list(1001,c("x"),list(2)))))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  x[[1]][[3]]==2
})

checkTrue(hb.delete("testtable",1001))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  is.null(x[[1]][[2]]) && length(x[[1]][[3]])==0
})

