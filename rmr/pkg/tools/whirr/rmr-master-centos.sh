# rmr-master-centos.sh by Jeffrey Breen, based on rmr-master.sh
# original copyright attached:
#
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

sudo yum -y --enablerepo=epel install R R-devel

sudo R --no-save << EOF
install.packages(c('RJSONIO', 'itertools', 'digest'), repos =  "http://lib.stat.cmu.edu/R/CRAN")
EOF

# install the rmr package from RHadoop:

branch=master

wget --no-check-certificate https://github.com/RevolutionAnalytics/RHadoop/tarball/$branch -O - | tar zx
mv RevolutionAnalytics-RHadoop* RHadoop
sudo R CMD INSTALL RHadoop/rmr/pkg/

sudo su << EOF1 
cat >> /etc/profile <<EOF
 
export HADOOP_HOME=/usr/lib/hadoop

EOF
EOF1
