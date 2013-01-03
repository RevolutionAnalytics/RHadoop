#get a fresher ant than yum would
wget --no-check-certificate http://apache.cs.utah.edu//ant/binaries/apache-ant-1.8.4-bin.tar.gz
tar xvzf apache-ant-1.8.4-bin.tar.gz 
export ANT_HOME=/home/users/antonio/apache-ant-1.8.4
sudo yum install -y xml-commons-apis
sudo yum install -y gcc
sudo yum install -y lzo-devel
sudo yum install -y make
wget --no-check-certificate https://github.com/kambatla/hadoop-lzo/tarball/master
tar xvzf master
cd kambatla*
export CFLAGS=-m64
export CXXFLAGS=-m64
ant package
sudo cp build/hadoop-lzo-*.jar /usr/lib/hadoop/lib/
sudo mkdir -p /usr/lib/hadoop/lib/native/
sudo cp build/native/Linux-*-*/lib/libgplcompression.* /usr/lib/hadoop/lib/native/

sudo /etc/init.d//hadoop-0.20-tasktracker restart
