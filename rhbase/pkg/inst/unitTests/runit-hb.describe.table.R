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

 
## to be run after hb.new.table
hb.init(host='127.0.0.1',port=9090)
checkTrue(is.data.frame(x <- hb.describe.table("testtable")))
checkTrue(nrow(x)==3)
checkTrue(all(rownames(x)==c("x:","y:","z:")))
checkTrue(x["y:","compression"]=="GZ")
