\name{keyval}
\alias{keyval}
\alias{keys}
\alias{values}

\title{Create or extract Key-Value Pairs}
\description{Creates a key-value pair from its arguments or extracts keys or values from a list of pairs.}

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
\code{mapreduce}, which can also return NULL. A key-value pair are also appropriate arguments for the \code{to.dfs} function and are returned by 
\code{from.dfs}. \code{keys} and \code{values} extract a list of keys or values resp. from a list of key value pairs or a
vectorized key-value pair.} 

\examples{  
  keyval(1,2)
  keys(replicate(5, keyval(1,2), simplify = FALSE))
  values(replicate(5, keyval(1,2), simplify = FALSE))
}
