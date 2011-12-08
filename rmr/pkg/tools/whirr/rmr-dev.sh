branch=dev
sudo apt-get install -y r-base-core
sudo R --no-save << EOF
install.packages(c('methods', 'RJSONIO', 'itertools', 'digest'), repos =  "http://lib.stat.cmu.edu/R/CRAN")
EOF

curl  -L   https://github.com/RevolutionAnalytics/RHadoop/tarball/$branch | tar zx
mv RevolutionAnalytics-RHadoop* RHadoop
sudo R CMD INSTALL RHadoop/rmr/pkg/

sudo su << EOF1 
cat >> /etc/profile <<EOF
 
export HADOOP_HOME=/usr/lib/hadoop

EOF
EOF1