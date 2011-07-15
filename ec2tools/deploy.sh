#cluster-do R CMD BATCH RHadoop/ec2tools/R-installs.R

#git pull
RHadoop-sync
cluster-do env HADOOP_HOME=/usr/lib/hadoop HADOOP_CONF=/usr/lib/hadoop/conf  R CMD INSTALL RHadoop/rhstream/pkg/
