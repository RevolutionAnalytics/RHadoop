\name{rhdfs-filelevel}
\alias{hdfs.ls}
\alias{hdfslist.files}
\alias{hdfs.delete}
\alias{hdfs.rm}
\alias{hdfs.del}
\alias{hdfs.dircreate}
\alias{hdfs.mkdir}
\alias{hdfs.chmod}
\alias{hdfs.chown}
\alias{hdfs.file.info}
\alias{hdfs.exists}

\title{File and Directory Manipulation Commands for the HDFS}
\description{
  These functions help browse the HDFS, modify and delete files
}
\usage{
  hdfs.ls(path, recurse=FALSE,cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs"))
  hdfslist.files(path, recurse=FALSE,cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs"))
  hdfs.delete(path, cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs"))	
  hdfs.del(path, cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs"))	
  hdfs.rm(path, cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs"))	
  hdfs.dircreate(paths,fs=hdfs.defaults("fs"))			
  hdfs.mkdir(paths,fs=hdfs.defaults("fs"))			
  hdfs.chmod(paths,permissions="777",fs=hdfs.defaults("fs"))
  hdfs.chown(paths,owner="",group="",fs=hdfs.defaults("fs"))
  hdfs.file.info(path,fs=hdfs.defaults("fs"))
  hdfs.exists(path, fs=hdfs.defaults("fs"))
}

\arguments{
\item{path}{The path to a file location on the HDFS
    e.g. /airline/data/1987.csv}. In some cases it can be a character
  vector, see Details.
\item{recurse}{TRUE to recursively list subdirectories}
\item{cfg}{The Hadoop configuration object. The user can specify this
    for different Hadoop installations, though usually not needed}
\item{fs}{The Hadoop filesystem object. The user can specify this for
    different Hadoop installations, though usually not needed}
\item{permissions}{A character indicating the permissions of the file}
\item{owner}{A character indicating the owner of the file}
\item{group}{A character indicating the group to which the file belongs}
}

\details{ 
  None of the functions described below accepts wildcards.
  Except for \code{hdfs.file.info} all functions accept character vectors for
  \code{path}.

  \code{hdfs.ls} returns a data frame with columns corresponding to
  'permissions','owner','group','size' (in bytes),'modification time' (a
  character) and 'file' (the path of the entry). If \code{recurse} is TRUE the
  function will recurse through subdirectories.
  \code{hdfs.list.files} is a synonym for \code{hdfs.ls}

  \code{hdfs.delete} deletes a file or directory (indicated by \code{path}). If
  \code{path} is a directory it will delete the directory and everything below
  it.
  \code{hdfs.rm} and \code{hdfs.del} are synonyms for \code{hdfs.delete}

  \code{hdfs.chown} and \code{hdfs.chmod} change the file permissions and owner
  and group membership of the file/directory. See
  \url{http://hadoop.apache.org/common/docs/r0.20.2/hdfs_permissions_guide.html}
  for further details

  \code{hdfs.dircreate} creates a directory. \code{hdfs.mkdir} is a synonym
  for \code{hdfs.dircreate}.

  \code{hdfs.file.info} is the HDFS version of the \code{file.info}
  function. It returns a data frame with columns 'perms', 'isDir', 'block', '
  'replication', 'owner', 'group', 'size', 'modtime', and 'path'.
  
  \code{hdfs.exists} returns TRUE if the \code{path} exists and FALSE
  otherwise.
}

\value{
  For \code{hdfs.ls} a data frame described in 'details'. For \code{hdfs.exists}
  TRUE or FALSE depending on whether the file exists. 
 
  \code{hdfs.delete} and its synonyms and \code{hdfs.createdir} and its synonym
  return TRUE if the operation succeeds and return an error if the operation fails.

  \code{hdfs.chmod} and \code{hdfs.chown} return NULL.
}

\examples{
  ## returns a data frame of the root folder
  hdfs.ls("/")

  foo1 = tempfile()
  foo2 = tempfile()

  writeLines("foo1", con = foo1)
  writeLines("foo2", con = foo2)

  hdfs.put(foo1, "/")
  hdfs.put(foo2, "/")

  ## delete multiple files
  hdfs.delete(c(foo1, foo2))

  writeLines("foo1", con = foo1)
  writeLines("foo2", con = foo2)

  hdfs.put(foo1, "/")
  hdfs.put(foo2, "/")

  ## change permissions of some files
  ## permissions wil be recycled
  hdfs.chmod(c(foo1, foo2), permissions=c("777","774"))

  ## file information, only accepts one path
  hdfs.file.info(foo1)
}

\seealso{
  \code{hdfs.copy},
  \code{hdfs.move},
  \code{hdfs.rename},
 \code{hdfs.put},
 \code{hdfs.get}
}
