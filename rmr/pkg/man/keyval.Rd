\name{keyval}
\alias{keyval}
\alias{keys}
\alias{values}

\title{Create or extract Key-Value Pairs}
\description{Creates a key-value pair from its arguments or extracts keys or values from a list of pairs.}

\usage{
  keys(l)
  values(l)
  keyval(k, v)
}

\arguments{
\item{l}{list containing key/value pairs}
\item{k}{the desired key or a list when v is missing}
\item{v}{the desired value}
}

\details{The keyval helper function should be used to create return values for the map and reduce functions passed as parameters to
\code{mapreduce}, which can also return NULL or a list of key-value pairs. Lists of keyvalue pairs are also appropriate arguments for the \code{to.dfs}
function and are returned by \code{from.dfs}. \code{keys} and \code{values} extract a list of keys or values resp. from a list of key value pairs}

\examples{  
  keyval(1,2)
  keys(replicate(5, keyval(1,2), simplify = FALSE))
  values(replicate(5, keyval(1,2), simplify = FALSE))
}
