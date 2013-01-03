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

branch=dev
sudo apt-get install -y r-base-core
sudo apt-get install -y r-cran-rcpp
sudo R --no-save << EOF
install.packages(c('RJSONIO', 'digest', 'functional', 'stringr', 'plyr','Rcpp'), repos = "http://cran.us.r-project.org")
EOF

rm -rf $branch RHadoop
curl  -L   https://github.com/RevolutionAnalytics/RHadoop/tarball/$branch | tar zx
mv RevolutionAnalytics-RHadoop* RHadoop
sudo R CMD INSTALL --byte-compile RHadoop/rmr2/pkg/

sudo su << EOF1 
echo ' 
export HADOOP_HOME=/usr/lib/hadoop
' >> /etc/profile 
 
EOF1
