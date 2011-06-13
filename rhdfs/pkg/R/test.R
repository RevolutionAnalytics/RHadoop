RunAllUnitTests <- function (showHTMLOutput = FALSE, filename = "RevoHDFSRUnitTestSummary.html")
{
    .failureDetails <- function(result) {
        res <- result[[1L]]
        if (res$nFail > 0 || res$nErr > 0) {
            Filter(function(x) length(x) > 0, lapply(res$sourceFileResults,
                function(fileRes) names(Filter(function(x) !(x$kind %in%
                  c("success", "deactivated")), fileRes))))
        }
        else {
            list()
        }
    }
    require("RUnit", quietly = TRUE) || stop("RUnit package not found")
    RUnit_opts <- getOption("RUnit", list())
    RUnit_opts$verbose <- 0L
    RUnit_opts$silent <- TRUE
    RUnit_opts$verbose_fail_msg <- TRUE
    options(RUnit = RUnit_opts)
    testDirs <- system.file("unitTests", package="RevoHDFS") 
    testSuite <- defineTestSuite(name = "RevoHDFS RUnit Tests",
        dirs = testDirs, rngKind = "default", rngNormalKind = "default")
    testResult <- runTestSuite(testSuite)
    if (showHTMLOutput) {
        fname <- filename
        printHTMLProtocol(testResult, fileName = fname)
        if (interactive()) {
            browseURL(paste("file:/", getwd(), fname, sep = "/"))
        }
    }
    cat("\n\n")
    printTextProtocol(testResult, showDetails = FALSE)
    if (length(details <- .failureDetails(testResult)) > 0) {
        cat("\nTest files with failing tests\n")
        for (i in seq_along(details)) {
            cat("\n  ", basename(names(details)[[i]]), "\n")
            for (j in seq_along(details[[i]])) {
                cat("    ", details[[i]][[j]], "\n")
            }
        }
        cat("\n\n")
        stop("unit tests failed for package RevoHDFS")
    }
    status <- getErrors(testResult)
    invisible(status$nFail == 0 && status$nErr == 0)
}

