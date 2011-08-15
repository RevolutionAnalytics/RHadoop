\name{keyval}
\alias{keyval}
\alias{getKeys}
\alias{getValues}

\title{Create or extract Key-Value Pairs}
\description{Creates a key-value pair from its arguments or extracts keys or values from a list of pairs.}

\usage{
  getKeys(l)
  getValues(l)
  keyval(k, v = NULL, i = 1)
}

\arguments{
\item{l}{list containing key/value pairs}
\item{k}{the desired key or a list when v is missing}
\item{v}{the desired value}
\item{i}{when v is missing, which element of the list to use as key}
}

\details{The keyval helper function should be used to create return values for the map and reduce functions passed as parameters to revoMapReduce, which can also return a list of key-value pairs. Lists of keyvaly pairs are also appropriate arguments for the rhwrite function and returned by rhread. When provided the first two arguments, the first is the key and the second the value. When the second argument is missing, the first must be a list, of which one element will become the key and the rest the value. Which element becomes the key is determined by the third, optional argument. getKeys and getValues extract a list of keys or values resp. from a list of key value pairs}

\examples{  
  keyval(1,2)
  keyval(list(1,2))
  keyval(list(1,2),i=2)
  getKeys(replicate(5, keyval(1,2), simplify = FALSE))
  getValues(replicate(5, keyval(1,2), simplify = FALSE))
}
