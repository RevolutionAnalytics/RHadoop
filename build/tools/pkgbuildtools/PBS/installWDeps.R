pkg <- commandArgs(TRUE)[[1]]
tmp <- installed.packages()
found <- match(pkg, names(tmp[,1]))
if (is.na(found))
{
	repos <- commandArgs(TRUE)[[2]]
	repos
	options(repos=repos)
	ans <- install.packages(pkg, type="source")
	tmp <- installed.packages()
	found <- match(pkg, names(tmp[,1]))
	if (is.na(found))
	{
		stop(pkg, ": FAILED to install\n")
	} else {
		cat(pkg, ": Installed successfully at ", tmp[found,2], "\n")
	}
} else 
{
	cat("Already installed:", tmp[found,2], "\n")
}