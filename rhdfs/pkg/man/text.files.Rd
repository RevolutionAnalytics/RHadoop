\name{rhdfs-text-manip}
\alias{hdfs.line.reader}
\alias{hdfs.read.text.file}

\title{Reading Text Files from the HDFS}
\description{
  Functions that read lines of text from the HDFS.
}
\usage{
hdfs.line.reader(path,n=1000L,buffer=as.integer(5*1024^2),fs=hdfs.defaults("fs"))
hdfs.read.text.file(path,...)
}
\arguments{
\item{path}{Location to file on HDFS to read}
\item{n}{Number of lines to read}
\item{buffer}{The size of the read buffer}
\item{fs}{The filesystem on which the file is located}
}

\details{ 
  \code{hdfs.line.reader} reads chunks of lines from a text file.
  

  \code{hdfs.read.text.file} reads the entire text file as one character vector.

  This function uses the Java method \code{readLine}. New lines are indicated by
  one of: a line feed ('\\n'), a carriage return ('\\r') or both ('\\r\\n').

  These can be used to compute external memory regression using the
  \code{bigglm} package. See the examples.
}

\examples{
   ##	This is from the source of hdfs.read.text.file
   path = tempfile()
   writeLines("line reader test", con = path)
   hdfs.put(path, "/tmp")
   m <- hdfs.line.reader(path)
   collector <- NULL
   while (length(lines <- m$read()) > 0) collector <- c(collector, 
     lines)
   m$close()
}
