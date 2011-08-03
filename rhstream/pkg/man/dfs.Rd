\name{dfs}
\alias{dfs}
\alias{dfs.ls}
\alias{dfs.get}
\alias{dfs.put}
\alias{dfs.rm}
\alias{dfs.rmr}
\alias{dfs.exists}

\title{Utility functions for working with HDFS}
\description{Utility functions for working with HDFS
}

\usage{
	dfs(cmd, ...)
	dfs.ls(...)
	dfs.get(...)
	dfs.put(...)
	dfs.rm(...)
	dfs.rmr(...)
	dfs.exists(f)
}

\arguments{
  \item{cmd}{hadoop dfs command to execute}
  \item{...}{any arguments and options to the corresponding hadoop dfs command}
  \item{f}{file to be tested}}

\details{ }
  
\examples{  
#	dfs.rmr("/tmp/oldexamples")
}
