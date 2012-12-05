# What's new in 2.0.2

## Fixes
* Fixed a bug whereby data frames in outer joing wouldn't work.
* Fixed a bug whereby only  the first column of a data.frame used as key would be available in the reducer.
* Updated the equijoin reduce default to work with the 2.x API.
* Updated equijoin to work correctly on the output of previous MR jobs.
* Cleaned up some stray debuggin statements that polluted stderr.
* Made equijoin resilient to the case where there are no records to join from either side for a particular key.
* Now compatible with the latest Rcpp (0.10.1) hence the latest R (2.15.2)

## Dependencies
* Removed dependency on httr which is still a little too bleeding edge and not available everywhere, besides having a number of dependencies of its own (thanks @hadley for letting us take and modify one needed function from httr).
