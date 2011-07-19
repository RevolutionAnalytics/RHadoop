\name{keyval}
\alias{getKeys}
\alias{getValues}

\title{Create Key-Value Pairs}
\description{Creates a key-value pair from its arguments.}
	
\usage{
	getKeys = function(l)
	getValues = function(l)
	keyval = function(k, v = NULL, i = 1)
}

\arguments{
  \item{l}{list containing key/value pairs}
  \item{k}{the desired key or a list when v is missing}
  \item{v}{the desired value}
  \item{i}{when v is missing, which element of the list to use as key}
}

\details{This helper function should be used to create return values for the map and reduce functions passed as parameters to revoMapReduce, which can also return a list of key-value pairs. Lists of keyvaly pairs are also appropriate arguments for the rhwrite function and returned by rhread. When provided the first two arguments, the first is the key and the second the value. When the second argument is missing, the first must be a list, of which one element will become the key and the rest the value. Which element becomes the key is determined by the third, optional argument}

\examples{  
> keyval(1,2)
$key
[1] 1

$val
[1] 2

attr(,"keyval")
[1] TRUE
> keyval(list(1,2))
$key
[1] 1

$val
$val[[1]]
[1] 2


attr(,"keyval")
[1] TRUE
> keyval(list(1,2),i=2)
$key
[1] 2

$val
$val[[1]]
[1] 1


attr(,"keyval")
[1] TRUE

}