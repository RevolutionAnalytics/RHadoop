# Scalable Analytics in R with rmr

*RHadoop* is an open source project started by Revolution Analytics to provide data scientists using R access to Hadoop’s scalability without giving up their favorite language flexibility and convenience.

So far it has three main packages:

* rhdfs provides file level manipulation for HDFS, the Hadoop file system
* rhbase provides access to HBASE, the hadoop database
* rmr allows to write mapreduce programs in R. This will be the focus of this presentation.

rmr allows R developers to program in the mapreduce framework, and to all developers provides an alternative way to implement mapreduce programs that strikes a delicate compromise betwen power and usability. It allows to write general mapreduce programs, offering the full power and ecosystem of an existing, established programming language. It doesn’t force you to replace the R interpreter with a special run-time $mdash;it is just a library. You can write logistic regression in half a page and even understand it. It feels and behaves almost like the usual R iteration and aggregation primitives. It is comprised of a handful of functions with a modest number of arguments and sensible defaults that combine in many useful ways. But there is no way to prove that an API works: one can only show examples of what it allows to do and we will do that covering a few from machine learning and statistics. Finally, we will discuss how to get involved.
