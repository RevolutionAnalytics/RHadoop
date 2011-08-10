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
checkTrue(hb.new.table("dframe","x","y","z"))
N <- 10000
p <- data.frame(x=runif(N),y=sample(1:N,N,replace=TRUE), z=sample(letters,N,replace=TRUE))
checkTrue(hb.insert.data.frame("dframe",p))
hb.delete.table("dframe")

