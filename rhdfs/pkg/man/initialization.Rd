\name{hdfs.init}
\alias{hdfs.init}
\docType{package}

\title{Initialize the rhdfs Package}
\description{
  Initializes the rhdfs package with paramaters specifying the location of the
  Hadoop configuration files
}

\usage{
  hdfs.init(hadoop=NULL)
}
\arguments{
\item{hadoop}{a character string specifying the complete path to the hadoop 
      binary (i.e. \code{/usr/bin/hadoop} ), defaults to the environment variable HADOOP_CMD}
}
\details{
  The \code{hdfs.init} function is called with no arguments as part of the
  normal on-load procedure for rhdfs; if HADOOP_CMD is unset, 
  the load will fail. It may be called multiple times per session, so
  if something about your configuration changes during a session, you can call
  \code{hdfs.init} again to pick up the changes.
}
\examples{
  ## uses default arguments
  hdfs.init()

  ## specify locations
  ##hdfs.init(hadoop="/usr/bin/hadoop")
}





