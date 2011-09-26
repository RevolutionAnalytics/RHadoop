\name{rhdfs-file-manip}
\alias{hdfs.file}
\alias{hdfs.write}
\alias{hdfs.close}
\alias{hdfs.flush}
\alias{hdfs.read}
\alias{hdfs.seek}
\alias{hdfs.tell}


\title{Reading and Writing to Files on the HDFS}
\description{
  Functions described here can be used to read and write to/from files on the
  HDFS. It is mostly implemented in streaming fashion (i.e. open, several
  updates, close) rather than one shot.
}
\usage{
hdfs.file(path,mode="r",fs=hdfs.defaults("fs"),buffersize=5242880,overwrite=TRUE
                     ,replication=hdfs.defaults("replication"),blocksize=hdfs.defaults("blocksize"))
hdfs.write(object,con,hsync=FALSE)
hdfs.close(con)
hdfs.flush(con)
hdfs.read(con, n,start)
hdfs.seek(con,n=0)
hdfs.tell(con)
}

\arguments{
\item{path}{Location to file on HDFS to read or write}
\item{mode}{'r' for reading and 'w' for writing. Appends are not allowed.}
\item{fs}{The filesystem to write files to. To write to local filesystems use hdfs.defaults("local")}
\item{buffersize}{The read/write buffer to use}
\item{replication}{For files opened in mode "w", the replication factor. Only
    makes sense for the HDFS}
\item{blocksize}{The blocksize of the written files}
\item{object}{The R object to be written to disk, see Details}
\item{con}{An open HDFS connection returned by \code{hdfs.file}}
\item{hsync}{If TRUE, the file will be synched after writing}
\item{n}{Number of bytes to read}
\item{start}{The position to read from,default is the current position}
}

\details{ 
  The functions can be used to read and write files both on the local filesystem
  and the HDFS. 
  If the object is a raw vector, it is written directly to the \code{con}
  object, otherwise it is serialized and the bytes written to the \code{con}. No
  prefix (for example, length of bytes) are written and it is up to the user to handle
  this.
  \code{hdfs.seek} seeks to the position \code{n}. It must be positive.
  \code{hdfs.tell} returns the current location of the file pointer.
}

\value{
  \code{hdfs.file} returns an object of class \code{hdfsFH} which has generic
  functions for \code{as.character} and \code{print}.  
  It has the following useful members:
  \item{blocksize}{blocksize of the opened file}
  \item{buffersize}{the size of the buffer for the opened file}
  \item{fh}{the filehandle for the open while. It is of class
      \code{org.apache.hadoop.fs.FSDataOutputStream}}
  \item{fs}{the filesystem on which the file resides}
  \item{mode}{the mode of the file}
  \item{name}{the name of the file}
  \item{replication}{the replication factor of the file}

  \code{hdfs.read} returns a raw vector or NULL if no more data is to be read.
}

\examples{
 ## Following example describes a way to lazy load a character of vector of 
 ## variables
 save.objects <- function(ob.names=ls(theenv), save.name, theenv=.GlobalEnv){
  hdfs.dircreate(save.name)
  data.file <- hdfs.file(path=sprintf("\%s/data",save.name),mode="w")
  dict.file <- hdfs.file(path=sprintf("\%s/dict",save.name),mode="w")
  on.exit({
      hdfs.close(data.file)
      hdfs.close(dict.file)
    })
  for(aname in ob.names){
    ## Write the name and its position in the dictionary
    current.position <- hdfs.tell(data.file)
    dict.file$fh$writeInt(as.integer(current.position))
    dict.file$fh$writeUTF(aname)
    ## Write the length of the object and its position in the data file
    obj <- get(aname, env=theenv)
    obj.sz <- serialize(obj,NULL)
    ## the following line uses the DataOutputStream writeInt method to write 4
    ## bytes. Rs writeBin does not write to DataOutputStream
    data.file$fh$writeInt(length(obj.sz)) 
    hdfs.write(obj.sz,data.file)
    hdfs.flush(data.file)
  }
  dict.file$fh$writeInt(as.integer(-1L))
}
load.objects <- function(from){
  dict.file <- hdfs.file(path=sprintf("\%s/dict",from),mode="r")
  inp <- J("java.io.DataInputStream")
  on.exit({
    hdfs.close(dict.file)
  })
  posn <- list()
  en <- parent.frame()
  repeat{
    pos <- dict.file$fh$readInt()
    if(pos<0) break
    nam <- inp$readUTF(dict.file$fh)
    posn[[ nam ]] <- list(nam,pos)
  }
  lapply(1:length(posn),function(r) {
    print(posn[[r]])
    delayedAssign(posn[[r]][[1]],
                  value=
                  {
                    message(sprintf("Lazy Loading '\%s'", posn[[r]][[1]] ))
                    data.file <- hdfs.file(path=sprintf("\%s/data",from),mode="r")
                    cat(sprintf("seeking to \%s\n",posn[[r]][[2]]))
                    hdfs.seek(data.file, posn[[r]][[2]])
                    cat(sprintf("Reading from \%s\n",hdfs.tell(data.file)))
                    nob <- data.file$fh$readInt()
                    temp <- unserialize(hdfs.read(data.file, nob))
                    hdfs.close(data.file)
                    temp
                  },assign.env=en)
  })
}
## Run within a fresh (clean) session
library(rhdfs)
hdfs.init()
x=runif(100)
save.objects(save.name="/tmp/asavefile")
rm(x)
load.objects("/tmp/asavefile")
x
}

\seealso{
  \code{hdfs.line.reader},\code{hdfs.read.text.file}
}
