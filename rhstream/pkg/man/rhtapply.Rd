\name{rhtapply}
\alias{rhtapply}

\title{Distributed tapply}
\description{
	Applies the \code{tapply} function to an external data set.
}
	
\usage{
rhtapply <- function(X,INDEX,FUN,data,colsep,mpr.out="!",...,debug=FALSE)	
}

\arguments{
\item{X}{A vector of column numbers. These columns will be provided as a character matrix to \code{FUN}}
\item{INDEX}{A vector of column numbers. The data set will be partitioned on unique values of these columns}
\item{FUN}{A function that takes a data frame and the stringified version of unique levels of columns in \code{INDEX}}
\item{data}{The path to text files on the HDFS}
\item{colsep}{An expression evaluated once for the slice}
\item{mpr.out}{Seperates the keys from the values}
\item{debug}{TRUE for some helpful output}
}

\details{ 
This function is similar to the \code{tapply} command found in R. The major difference is that the function \code{FUN}
does not get all rows of the data set corresponding to the level combinations of \code{INDEX} at once. It gets them in chunks.
The function \code{FUN} receives the key (a stringified version of the unique levels) and a data frame whose columns correspond to the
columns specified in \code{X}.
}

\value{
	Typically a data frame with with the unique levels of the \code{INDEX} columns and the colums for the data returned by \code{FUN}.
}

\examples{
## Count # of  rows
## Year(1),Month(2), and DayOfMonth(4), ArrDelay is the 15'th column
## All indices in X are converted to Numeric
## From: http://stat-computing.org/dataexpo/2009/the-data.html
y <- rhtapply( X=c(15),INDEX=c(1,2,4),FUN=function(k,d)
             {
               Sum(k, nrow(d))
             },data="/airline/data/1987.csv",colsep=",+")


## Histogram Information: Min,Max, Mean, Std
y <- rhtapply( X=c(15),INDEX=c(1,2,4),FUN=function(k,d)
             {
               apply(d,1,function(r) Hist(k, r[1]))
             },data="/airline/data/1987.csv",colsep=",+")

## Example 5: Sum of Square of ArrDelay
y <- rhtapply( X=c(15),INDEX=c(1,2,4),FUN=function(k,d)
             {
               Sum(k, sum(d[,1]^2))
             },data="/airline/data/1987.csv",colsep=",+")

	 
}
