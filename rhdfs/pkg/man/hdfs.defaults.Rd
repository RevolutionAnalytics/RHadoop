\name{hdfs.defaults}
\alias{hdfs.defaults}
\title{Retrieve and Set RevoHDFS Defaults}
\description{
  Use this function to retrieve RevoHDFS defaults.
}
\usage{
  hdfs.defaults(arg)
}
\arguments{
\item{arg}{The name of one of parameter, or empty to return all defaults}
}
\value{
        A list with the following components (most of which are not important to the user)
        \item{local}{An rJava object corresponding to the local filesystem.}
        \item{blocksize}{The default block size of files stored on the HDFS.}
        \item{fs}{An rJava object corresponding to HDFS}
        \item{fu}{Helper object for RevoHDFS}
        \item{replication}{The default replication count for files stored on the HDFS}
        \item{conf}{An rJava object which contains name-value mappings for Hadoop configuration parameters.}
}

\examples{
  ## returns all the default objects
  hdfs.defaults()

  ## get mapred.map.tasks
  hdfs.defaults("conf")[["mapred.map.tasks"]]
}

