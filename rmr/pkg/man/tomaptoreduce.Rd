\name{to.map}
\alias{to.map}
\alias{to.reduce}
\alias{to.reduce.all}
\title{Create map and reduce functions from other functions}

\description{These utility functions are meant to avoid the little boilerplate code necessary to convert ordinary functions into map and
reduce functions.}

\usage{
	to.map(fun1, fun2 = identity)
	to.reduce(fun1, fun2 = identity)
	to.reduce.all(fun1, fun2 = identity)
}

\arguments{
  \item{fun1}{a function to apply to the key, or to the key, value pair if the second argument is missing}
  \item{fun2}{a function to apply to the value}
}

\details{Sometimes there are functions that we could use almost directly as map or reduce functions but for a bit of boilerplate code, and
we hate boilerplate code. That's where the functions documented herein can help. They take one or two functions of a single argument and
return a valid map or reduce function. In the case of \code{to.map} when two functions are specified they are applied independently to the
key and the value and the return values are returned as a key-value pair; when only one is, it is applied to the key-value pair. For
\code{to.reduce} the behavior is the same with the only difference the a single value is replaced by a list of values. In the case of
\code{to.reduce.all} the function(s) are applied to the key and value for each value in the list passed to reduce.}




\examples{ 
##The identity map: 
to.map(identity) 
## equivalent to function(k,v) keyval(k,v) 
##Replace key with mod 10 of the key and pass the value along: 
to.map(function(x) x \%\% 10, identity ) 
##Sum up all the values for the same key: 
to.reduce(identity, function(vv) sum(unlist(vv)))
##return all the pairs k, sqrt(v) for each v associated with k
to.reduce.all(identity, sqrt)}
