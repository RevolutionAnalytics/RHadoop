\name{rhlapply}
\alias{rhlapply}

\title{Distributed lapply}
\description{
	Applies a function to the elements of a list
}
	
\usage{
	rhlapply(NorList,FUN,chunk.size=0,pre=NULL,debug=FALSE,.aggr=NULL,mapred=list(),mpr.out="!",ret.sep=",",keep=FALSE,...)
}

\arguments{
\item{NorList}{An integer N (which is a synonym for \code{1:n})}
\item{FUN}{A function that takes one argument}
\item{chunk.size}{For large N, the number of slices of 1:N, each of which are run sequentially}
\item{pre}{An expression evaluated once for the slice}
\item{debug}{TRUE for some helpful output}
\item{.aggr}{A function to aggregate the results, see Details}
\item{mapred}{A list of Hadoop mapreduce options}
\item{mpr.out}{Separates the keys from the values}
\item{ret.sep}{Separates the fields in the value, e.g. which is only useful if the Hist function is used}
\item{keep}{If TRUE, the temporary output file on the HDFS is not deleted}
}

\details{ 
This function can be used to apply the function \code{FUN} to the numbers 1,2,...,N.	  	  
}

\value{A list with components giving the result of the function on each of the numbers 1,2, ..., N.
}

\examples{

# Generates 10 random uniform numbers 10 times.
# 3 parallel runs each run 3 calls sequentially

rhlapply(10,
         FUN=function(r) {
           runif(10)
         },chunk.size=3,keep=TRUE)

## F(r) returns r uniform random numbers
## Uses the mapred argument to run on a local system
## the .aggr combines the new 'values' to the existing 'current'
## 'indices' corresponds to the index from 1...N
rhlapply(10,
         FUN=function(r) {
           runif(r)
         }
         ,.aggr = function(values,current, indices) {
           a <- lapply(1:length(values),function(r)
                       list(as.numeric(indices[[r]]),as.numeric(values[[r]]))
                       )
           append(a,current)
         },debug=TRUE,
         ,mapred=list(mapred.job.tracker='local'))
	 
}
