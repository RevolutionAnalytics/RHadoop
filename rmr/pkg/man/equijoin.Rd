\name{equijoin}

\alias{equijoin}

\title{
Equijoins using map reduce
}

\description{
A generalized form of equijoin, hybrid between the SQL brethen and mapreduce
}

\usage{equijoin(left.input = NULL, right.input = NULL, input = NULL, output = NULL, outer = c("", "left", "right", "full"), map.left =
to.map(identity), map.right = to.map(identity), reduce = function(k, values.left, values.right) do.call(c, lapply(values.left, function(vl)
lapply(values.right, function(vr) reduceall(k, vl, vr)))), reduceall = function(k, vl, vr) keyval(k, list(left = vl, right = vr)))}

\arguments{\item{left.input}{The left side input to the join.}
  \item{right.input}{The right side input to the join.}
  \item{input}{The only input in case of a self join. Mutually exclusive with the previous two.}
  \item{output}{Where to write the output.}
  \item{outer}{Whether to perform an outer join, one of the usual three types, left, right or full.}
  \item{map.left}{Function to apply to each record from the left input, follows same conventions as any map function. The returned key
will become the join key.}
  \item{map.right}{Function to apply to each record from the right input, follows same conventions as any map function. The returned key
will become the join key.}
  \item{reduce}{Function to be applied, key by key, on the lists of values associated with that key, one from the right side as produced
by map.left and the other from the right side and map.right. This is a more mapreduce like API, very much like a reduce but with two lists
of values, all sharing the same key. Returns 0 or more keyval pairs like any reduce function.}
  \item{reduce.all}{Function to be applied to each triple comprising a key, a value associated with that key from the left input and a
value associated with that key from the right input. This is more an SQL like interface, like an equijoin. Returns 0 or more keyval pairs
like any reduce function.}
}

\value{If output is specified, returns output itself. Otherwise, an object that can be used as input to \code{\link{from.dfs}} or
\code{mapreduce}

}


\section{Warning}{Doesn't work with multiple inputs like \code{mapreduce}}


\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.
 from.dfs(equijoin(left.input = to.dfs(lapply(1:10, function(i) keyval(i, i^2))), right.input = to.dfs(lapply(1:10, function(i) keyval(i, i^3)))), to.data.frame=TRUE)
}