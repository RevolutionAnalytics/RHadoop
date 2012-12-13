\name{keyval}
\alias{keyval}
\alias{keys}
\alias{values}
\alias{c.keyval}

\title{Create, project or concatenate key-value pairs}
\description{Create a key-value object (a collecton of key-value pairs) from two R objects, extract keys or values from a key value object or concatenate multiple key value objects}

\usage{
  keys(kv)
  values(kv)
  keyval(key, val)
  c.keyval(...)
}

\arguments{
\item{kv, kv1}{key-value pairs}
\item{key}{the desired key or keys}
\item{val}{the desired value or values}
\item{...}{key-value pairs to concatenate}}

\details{The \code{keyval} function is used to create return values for the map and reduce functions, themselves parameters to
\code{mapreduce}. Key-value objects are also appropriate arguments for the \code{to.dfs} function and are returned by 
\code{from.dfs}. \code{keys} and \code{values} extract keys  and values resp. from a key value object. \code{c.keyval} concatenates two or more key-value objects by concatenating the keys and values separately after recycling the arguments. When invoked with a single argument, it considers it a list of key value objects to concatenate. A key value object should always be considered vectorized, meaning that it defines a collection of key-value pairs. For the purpose of forming key-value pairs, the length of an object is considered its number of rows whene defined, that is for matrices and data frames, or its R \code{length} otherwise. Consistently with this definition, the n-th element of a key or value  argument is its n-th row or a subrange including only the n-th element otherwise. Data types are preserved, meaning that, for instance, if the \code{key} is a matrix its n-th element is a matrix with only one row, the n-th row of the larger matrix (the behavior of the \code{[]} operator with \code{drop = FALSE}). The same is true for data frames, list and atomic vectors. When \code{key} and \code{val} have different lengths according to this definition, recycling is applied. The pairing between keys and values is positional, meaning that the n-th element of the key argument is associated with the n-the value of the val argument in a single key-value pair. Concatenation happens with \code{rbind} or variants thereof whenever keys or values have rows, \code{c} otherwise. Mxing and matching keys of different type, e.g. a matrix with a vector, is not supported, and the same is true for keys, but key and value in the same keyval object do not need to be of the same type. }


\examples{
#single key-val
  keyval(1,2)
  keys(keyval(1,2))
  values(keyval(1,2))
#10 kv pairs of the form (i,i)
  keyval(1:10, 1:10)
#2 kv pairs (1, 2i-1) and  (2, 2i) for i in 1:5 
  keyval(1:2, 1:10)
# mtcars is a data frame, each row is a value with key set to the value of column cyl
  keyval(mtcars$cyl, mtcars)
# concatenate two keyval objects  
  c.keyval(keyval(1,1:2), keyval(1,1:3))
}
