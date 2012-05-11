\name{keyval}
\alias{keyval}
\alias{keys}
\alias{values}

\title{Create or extract Key-Value Pairs}
\description{Creates a key-value pair from its arguments or extracts keys or values from a list of pairs.}

\usage{
  keys(kvl)
  values(kvl)
  keyval(k, v, vectorized = FALSE)
}

\arguments{
\item{kvl}{list containing key/value pairs}
\item{k}{the desired key or a list when v is missing}
\item{v}{the desired value}
\item{vectorized}{\code{k,v} are to be construed not as a single key-value pair but as a list of keys and list of values of equal length, to be matched positionally}
}

\details{The keyval function is used to create return values for the map and reduce functions passed as parameters to
\code{mapreduce}, which can also return NULL or a list of key-value pairs. The \code{vectorized} option is used for efficiency when many,
small key-value pairs need to be returned. Lists of keyvalue pairs are also appropriate arguments for the \code{to.dfs} function and are
returned by \code{from.dfs}. \code{keys} and \code{values} extract a list of keys or values resp. from a list of key value pairs}

\examples{  
  keyval(1,2)
  keys(replicate(5, keyval(1,2), simplify = FALSE))
  values(replicate(5, keyval(1,2), simplify = FALSE))
}
