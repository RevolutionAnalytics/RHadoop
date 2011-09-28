\name{rhdfs-file-manip}
\alias{hdfs.copy}
\alias{hdfs.move}
\alias{hdfs.rename}
\alias{hdfs.put}
\alias{hdfs.get}


\title{File Copying Commands for the HDFS}
\description{
  These functions copy,move and rename files between locations on the HDFS and
  to/from the local filesystem.
}
\usage{
  hdfs.copy(src,dest,overwrite=FALSE,srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs"))
  hdfs.move(src,dest,srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs"))
  hdfs.rename(src,dest)
  hdfs.put(src,dest,dstFS=hdfs.defaults("fs"))
  hdfs.get(src,dest,srcFS=hdfs.defaults("fs"))
}

\arguments{
\item{src}{Location of the source file/directory. See Details}
\item{dst}{Location of the destination file/directory.}
\item{srcFS}{The source file system.}
\item{dstFS}{The destination file system.}
}

\details{ 
  \code{hdfs.copy} copies several files (\code{src}) to the destination
  (\code{dest}). If the length of \code{src} is greater than 1, then \code{dest}
  must be a directory. The default is that both \code{src} and \code{dest} are
  located on the HDFS.

  \code{hdfs.move} works like \code{hdfs.copy} but deletes the original.

  \code{hdfs.put} is like \code{hdfs.copy} but \code{src} is on the local
  filesystem. Use this command to transfer files from the local filesystem to
  the HDFS. 

  \code{hdfs.get} copies files from the HDFS to the local filesystem.
  
  Both \code{hdfs.put} and \code{hdfs.get} overwrite the destination.

}

\value{
  TRUE if successful, FALSE otherwise.
}

\examples{
  hdfs.init()
  ## copies local /tmp/hdfs-test to tmp of the HDFS
  save.image("/tmp/hdfs-test")
  hdfs.put("/tmp/hdfs-test","/tmp")
  hdfs.ls("/tmp")
  ## moves /tmp/hdfs-test to /tmp/hdfs-test1 (both on HDFS)
  hdfs.move("/tmp/hdfs-test","/tmp/hdfs-test1")
}
