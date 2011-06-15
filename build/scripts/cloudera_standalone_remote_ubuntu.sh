# cloudera_standalone_remote.sh
# Justin Martenstein, April 2011

# this script runs on a remote Red Hat / CentOS 5 box, and will set up
# a stand-alone Cloudera Hadoop (CDH) system with Revo bits

# add the Cloudera repository
function add_repo() {
    if [ ! -f /etc/apt/sources.list.d/cloudera.list ]
    then
	RELEASE=  echo "deb http://archive.cloudera.com/debian $RELEASE-cdh3 contrib
deb-src http://archive.cloudera.com/debian $RELEASE-cdh3 contrib" > /etc/apt/sources.list.d/cloudera.list
	apt-get update
    fi
}

# download and install java
function install_java() {

   JAVA_VERSION=jdk-6u21
   arch=x64

   JDK_PACKAGE=$JAVA_VERSION-linux-$arch-rpm.bin
   JDK_INSTALL_PATH=/usr/java

   cd /tmp

   # download the package, sett the correct permissions
   echo "Downloading java: ${JDK_PACKAGE}"
   wget -q -nv http://whirr-third-party.s3.amazonaws.com/$JDK_PACKAGE
   chmod +x $JDK_PACKAGE

   # move the more script so the license doesn't display
   mv /bin/more /bin/more.tmp
   yes | ./$JDK_PACKAGE -noregister
   mv /bin/more.tmp /bin/more

   # set JAVA_HOME in the profile
   export JAVA_HOME=$(ls -d ${JDK_INSTALL_PATH}/jdk* | sort -rn | head -1)
   echo "export JAVA_HOME=$JAVA_HOME" >> /etc/bashrc

   /usr/sbin/alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 9999
   /usr/sbin/alternatives --set java $JAVA_HOME/bin/java
   java -version

}

# install Cloudera's Hadoop bits
function install_hadoop() {

   # make sure we have the new bashrc
   source /etc/bashrc

   # install
   yum -y install hadoop-0.20-conf-pseudo

   echo "export HADOOP_HOME=/usr/lib/hadoop" >> /etc/bashrc
   echo "export HADOOP_CONF=/etc/hadoop" >> /etc/bashrc

   # start the hadoop-related services
   for service in `ls /etc/init.d/hadoop-0.20-*`; do $service start; done

}

# install HBase for standalone
function install_hbase() {
   
   # install
   yum -y install hadoop-hbase
   yum -y install hadoop-hbase-master
   
   # start the services
   /etc/init.d/hadoop-hbase-master start

}

# install thrift bits
function install_thrift() {

   thrift_version=0.5.0

   # download the thrift source; we'll need this for the header files that
   # rhbase uses
   wget -q -nv -O /tmp/thrift.tar.gz http://apache.ziply.com/incubator/thrift/${thrift_version}-incubating/thrift-${thrift_version}.tar.gz
   cd /tmp/ && tar zxvf thrift.tar.gz
   cd thrift*
   yum -y install gcc-c++ boost-devel.x86_64 python-devel.x86_64 ruby-devel.x86_64
   ./configure --enable-gen-php=no --with-php=no --with-php_extension=no || exit
   make install
   #cd /tmp && rm -rf thrift*
   echo "/usr/local/lib" > /etc/ld.so.conf.d/thrift.conf
   echo export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/lib/pkgconfig/ >> /etc/bashrc
                   
   # install Cloudera's thrift bits, too
   yum -y install hadoop-hbase-thrift

   # start thrift
   /etc/init.d/hadoop-hbase-thrift start

}


# install Revo bits
function install_revo() {

   pushd /tmp

   # install community edition
   FILE=Revo-Co-4.3-RHEL5.tar.gz

   # list the hadoop packages
   HADOOP_PKGS="RevoHDFS_0.2.tar.gz 
                RevoHStream_0.2.tar.gz 
                RevoHBase_0.2.tar.gz"

   # download all the packages first, before we start installing
   echo "Downloading Revo R and Hadoop packages"
   wget -q -nv http://revo-whirr-downloads.s3.amazonaws.com/$FILE
   for pkg in $HADOOP_PKGS; do
      wget -q -nv http://revo-whirr-downloads.s3.amazonaws.com/$pkg.tar.gz
   done

   # unpack and install the main Revo R package
   tar xzf $FILE
   pushd RevolutionR_4.3; ./install.py -n -d; popd

   # make sure we have JAVA_HOME set correctly
   source /etc/bashrc
   sh -c "source /etc/bashrc && \
          Revo64 CMD javareconf && \
          Revoscript -e 'install.packages(\"rJava\")'"

   # install rhadoop packages
   for pkg in $HADOOP_PKGS; do
      Revo64 CMD INSTALL $pkg || exit
   done

   popd

   yum -y install yum-utils

}

add_repo
install_java
install_hadoop
install_hbase
install_thrift
install_revo
