\name{rmr.backend}
\alias{rmr.backend}

\title{Sets backend for mapreduce}
\description{
Sets the backend used for mapreduce}
\usage{
rmr.backend(backend = c(NULL, "hadoop", "local"))
}

\arguments{
  \item{backend}{
One of hadoop or local, the latter being implemented entirely in the current R interpreter.
}
}

\details{ Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to Hadoop through the hadoop backend and
provide an all-R, single interpreter implementation that's good for experimentation and debugging, in particular to debug mapper and
reducers. Can be specified as an argument to mapreduce or globally with this call.  }

\value{
The current backend as a string.
}

\examples{
rmr.backend()
rmr.backend("local")
rmr.backend("hadoop")
}