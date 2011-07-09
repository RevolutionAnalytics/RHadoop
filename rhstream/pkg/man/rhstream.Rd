\name{rhstream}
\alias{rhstream}

\title{MapReduce using Hadoop Streaming}
\description{
	Applies a Map R function to blocks of text data and then Reduces them using predefined aggregators or user R code
}
	
\usage{
rhstream <- function(map, reduce,in.folder,
                     out.folder, colspec=NULL,colsep = "[[:space:]]+",
                     N=2000,verbose=FALSE,numreduces,cachefiles=c()
                     ,archives=c(),jarfiles=c(),otherparams=list(),mapred=list()
                     ,mpr.out=NULL,ANYPREMAP={},ANYPREREDUCE={}
                     ,inputformat=NULL,debug=FALSE)
}

\arguments{
\item{map}{An R function that receives a matrix or a data frame or a list (depending on \code{colspec}) corresponding to the input}
\item{reduce}{A character or an R function. See Details}
\item{in.folder}{A path to the input data sets (on the HDFS)}
\item{out.folder}{The output destination, on the HDFS}
\item{colspec}{Like \code{colClasses} of \code{read.table}. It can also be a list of column number (in quotes e.g. '1') to one of 'character', 'integer', 'numeric' mappings.}
\item{colsep}{A character (maybe a regular expression) that indicates the column seperator}
\item{N}{The chunksize passed to the \code{map} functions}
\item{numreduces}{The number of reduces, equivalent to setting \code{mapred.reduce.tasks} in \code{mapred}}
\item{cachefiles,archives,jarfiles}{Character vectors of files to be copied to the local filsystem of the nodes and can be used in the R codes on the remote nodes}
\item{mpr.out}{The value of \code{mapred.textoutputformat.separator}, it is used by htapply}
\item{ANYPREMAP}{An R expression  called once for a block (a block is many chunks)}
\item{ANYPREREDUCE}{An R expression called once for a reduce task ( a single reduce task can process many keys). Only used if \code{reduce} is an expression}
\item{inputformat}{Can be the fully qualified Java class, in which case the JAR file must be passed via \code{jarfiles}}
\item{debug}{If TRUE some useful output is displayed}
}

\details{ This functions packages R code to run in a streaming fashion across a
Hadoop cluster. The input source is chuncked into blocks of text lines. These
text lines are split on column boundaries (specified by \code{colsep}). If
\code{colspec} is not given, the function is given a character matrix
corresponding to the split chunk of text. If \code{colspec} is NULL, a list of
character vectors (one per line) is returned which is useuful if rows do not
have the same number of columns. If colspec is a list
e.g. \code{list('1'='character', '4'='numeric')}, a data frame of a character
and numeric column corresponding to the first and fourth columns of the input
data set are returned.

This is given to the map function. To send data onto the Hadoop system, call
 \code{send} or \code{sendKV}. The former takes two character vectors
 corresponding to keys and values. \code{sendKV} takes a single chararacter
 vector, the first element of which is a key and the second is a value.

If \code{reduce} is the string 'aggregate', the user can use the functions
\code{Sum,Min,Max} and \code{Hist} (each of which take a character key and a
numeric value). These use Java based combiner to speed up the aggregation and
does away with the need of a custom R reduce function.

\code{ANYPREMAP, ANYPREREDUCE} are R expressions that are evaluated for a block
of text (a block consists of many chunks of text size N) and can be used to load
data sets which have been saved to the HDFS and their paths have been specified
in \code{cachefiles}.
}

\value{
NULL. The output is written to the output specified in \code{out.folder}
}

\examples{

## Example 0.
## Extract some lines
map <- function(d){
  send(c("a",Vec2Text(d[[1]])))
}
hdfs.del("/tmp/demo")
rhstream(map=map,in.folder="/airline/data/1987.csv",
         out.folder="/tmp/demo",verbose=TRUE,colsep=",+",mapred=list(mapred.job.tracker='local'))

## Example 1.
## Subset all rows that occur on a Thursday (DayOfWeek == 4)
## See http://stat-computing.org/dataexpo/2009/the-data.html for field numbers
## We need the "0" for the separator since Hadoop Streaming will force
## a tab if it is empty
map <- function(d){
  ## a data frame of 29 columns
  d <- d[d[,4]=="4",]
  s <- apply(d,1,function(r) send(c("",Vec2Text(r,collapse=","))))
}
hdfs.del("/tmp/demo")
rhstream(map=map,in.folder="/airline/data/1987.csv"
         ,out.folder="/tmp/demo",verbose=TRUE
         ,colsep=",+",colspec=rep('character',29)
         ,mapred=list(mapred.job.tracker='local',mapred.textoutputformat.separator="0"))


## Example 2.
## Counts the number of lines in a file
## The reduce="aggregate" enables the use of Sum,Max,Min and Hist
map <- function(d){
  k <- length(d)
  Sum("nrows",k)
}
hdfs.del("/tmp/demo")
rhstream(map=map, reduce="aggregate",in.folder="/airline/data/1987.csv",
         out.folder="/tmp/demo",verbose=TRUE,mapred=list(mapred.job.tracker='local'))


## Example 3.
## Subset all rows but keep only 
## Year,Month, and DayOfMonth
## To do this we want a data frame, with Year, Month and  DayOfWeek (1,2,4)
## From: http://stat-computing.org/dataexpo/2009/the-data.html
map <- function(d){
    s <- apply(d,1
               ,function(r){
                 send(c("",Vec2Text(r,collapse=",")))
               })
  }
colspec <- list("1"='character',"2"='character',"4"='character')
hdfs.del("/tmp/demo")
rhstream(map=map,
         ,in.folder="/airline/data/1987.csv"
         ,out.folder="/tmp/demo"
         ,colspec=colspec,colsep=",+"
         ,verbose=TRUE,mapred=list(mapred.job.tracker='local',mapred.textoutputformat.separator="0"))



}
