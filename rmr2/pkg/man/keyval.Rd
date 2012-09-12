\name{keyval}
\alias{keyval}
\alias{keys}
\alias{values}

\title{Create or extract Key-Value Pairs}
\description{Creates a key-value pair from its arguments or extracts keys or values from a key value pair}

\usage{
  keys(kv)
  values(kv)
  keyval(key, val)
}

\arguments{
\item{kv}{A key-value pair}
\item{key}{the desired key or keys}
\item{val}{the desired value or values}}

\details{The keyval function is used to create return values for the map and reduce functions passed as parameters to
\code{mapreduce}, which can also return NULL. Key-value pairs are also appropriate arguments for the \code{to.dfs} function and are returned by 
\code{from.dfs}. \code{keys} and \code{values} extract keys  and values resp. from a key value pair. A key value pair should be always considered vectorized, meaning that it defines a collection of key-value pairs. For the purposed of forming key-value pairs, the length of an object is considered its number of rows whene defined, that is for matrices and data frames, or its R \code{length} otherwise). Consistently with this definition, the n=th element of a key or value is its n-th row or a subrange including only the n-th element otherwise. Data types are preserved, meaning that, for instance, if the \code{key} is a matrix its n-th element is a matrix with only one row, the n-th row of the larger matrix (the behavior of the \code{[]} operator with \code{drop = FALSE}). The same is true for data frames, list and atomic vectors. When \code{key} and \code{val} have different lengths according to this definition, recycling is applied. The pairing between keys and values is modeled after the behavior of the function \code{\link{split}}, with some differences that will be detailed. This means that as many key value pairs are defined as there are \emph{distinct} keys. Each key will be associated with a subrange of \code{val} (subrange of rows when possible), specifically the subrange of \code{key} where each element is equal to \code{key}. The differences with split are the following:
\enumerate{
\item{\code{split} acts immediately whereas the generation of actual key-value pairs is deferred to serialization time for \code{keyval} and in general invisible to the user but for its effects on the behavior of mapreduce}
\item{\code{keyval} treats matrices as matrices, whereas \code{split} turns them into vector}
\item{\code{split} considers lists as data frames when provided as the grouping argument; \code{keyval} treats them as generic vectors when supplied as the key argument}}
}

\examples{
#single key-val
  keyval(1,2)
  keys(keyval(1,2))
  values(keyval(1,2))
#10 kv pairs of the form (i,i)
  keyval(1:10, 1:10)
#2 kv pairs (1, c(1,3,5,6,9)) and  (2, c(2,4,6,8,10))
  keyval(1:2, 1:10)
# split mtcars data according to cyl column, create several kv pairs
  keyval(mtcars$cyl, mtcars)
}
