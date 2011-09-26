\name{hdfs.init}
\alias{hdfs.init}
\title{Initialize the rhdfs Package}
\description{
  Initializes the rhdfs package with paramaters specifying the location of the
  Hadoop configuration files
}

\usage{
  hdfs.init(hadoop=NULL,conf=NULL,libs=NULL,contrib=NULL,verbose=FALSE)
}
\arguments{
\item{hadoop}{a character string specifying the installation directory containing the
    Hadoop files, defaults to the environment variable HADOOP_HOME}

  \item{conf}{a character string specifying the configuration directory containing the
      Hadoop configuration files, defaults to the environment variable
      HADOOP_CONF}

  \item{libs}{a character specifying the library directory containing the Hadoop
      libraries (jar files), defaults to \code{hadoop/libs}}

  \item{contrib}{a character specifying the 'contrib' directory containing the
      Hadoop contributed JAR files, defaults to \code{hadoop/contrib}}
    
    \item{verbose}{if TRUE, prints out some helpful information}
}
\details{
  The \code{hdfs.init} function is called with no arguments as part of the
  normal on-load procedure for rhdfs; if either HADOOP_HOME or HADOOP_CONF
  is unset, the load will fail. It may be called multiple times per session, so
  if something about your configuration changes during a session, you can call
  \code{hdfs.init} again to pick up the changes.
}
\examples{
  ## uses default arguments
  hdfs.init()

  ## specify locations
  \dontrun{
  hdfs.init(hadoop="/usr/local/hadoop", conf="/usr/local/etc/hadoop/conf")
}}





