wget https://github.com/kambatla/hadoop-lzo/tarball/master
tar xvzf master
cd kambatla*
export CFLAGS=-m64
export CXXFLAGS=-m64
sudo apt-get install -y ant
sudo apt-get install -y gcc
sudo apt-get install -y liblzo2-dev
sudo apt-get install -y make
export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-amd64
ant package
sudo cp build/hadoop-lzo-*.jar /usr/lib/hadoop/lib/
sudo mkdir -p /usr/lib/hadoop/lib/native/
sudo cp build/native/Linux-*-*/lib/libgplcompression.* /usr/lib/hadoop/lib/native/

sudo /etc/init.d//hadoop-*-tasktracker restart
