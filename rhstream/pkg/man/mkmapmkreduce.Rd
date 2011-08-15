\name{mkMap}
\alias{mkMap}
\alias{mkReduce}

\title{Create map and reduce functions from other functions}
\description{These utility functions are meant to avoid the little boilerplate code necessary to convert ordinary functions into map and reduce functions. Most of the time you might prefer writing map and reduce functions directly as function(k,v){<your code here>; keyval(x,y)}}

\usage{
	mkMap(fun1 = identity, fun2 = identity)
	mkReduce(fun1 = identity, fun2 = identity)
}

\arguments{
  \item{fun1}{a function to apply to the key, or to the key, value pair if the second argument is missing}
  \item{fun2}{a function to apply to the value}
}

\details{Sometimes there are functions that we could use almost directly as map or reduce functions but for a bit of boilerplate code, and we hate boilerplate code. That's where mkMap and mkReduce can help. If we pass only one argument, that function will be applied to the key-value pair as a list of two elements and the return value will be returned by the map or reduce function. If we pass two arguments, those functions will be applied independently to the key and the value resp and their return values returned as a key-value pair}
\examples{  
##The identity map: 
mkMap(identity)
## equivalent to 
function(k,v) keyval(k,v)
##Replace key with mod 10 of the key and pass the value along: 
mkMap(function(x) x \%\% 10, identity )
##Sum up all the values for the same key: 
mkReduce(identity, function(vv) sum(unlist(vv)))}
