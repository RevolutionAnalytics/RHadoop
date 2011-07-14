# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/bin

export PATH


export SSH_ASKPASS=""
export GIT_SSL_NO_VERIFY=true
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_CONF=/etc/hadoop/conf
export HADOOP=$HADOOP_HOME
