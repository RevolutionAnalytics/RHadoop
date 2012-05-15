export JAVA_HOME=/usr/lib/jvm/java-6-openjdk/
wget https://github.com/toddlipcon/hadoop-lzo/tarball/master
tar xvzf master
cd toddlipcon-hadoop-lzo-c7d54ff/
export CFLAGS=-m64
export CXXFLAGS=-m64
sudo apt-get install -y ant
sudo apt-get install -y gcc
sudo apt-get install -y liblzo2-dev
sudo apt-get install -y make
ant compile-native tar
sudo cp build/hadoop-lzo-0.4.15/hadoop-lzo-0.4.15.jar /usr/lib/hadoop/lib/
tar -cBf - -C build/hadoop-lzo-0.4.15/lib/native/ . | sudo tar -xBvf - -C /usr/lib/hadoop/lib/native/
sudo /etc/init.d//hadoop-0.20-tasktracker restart