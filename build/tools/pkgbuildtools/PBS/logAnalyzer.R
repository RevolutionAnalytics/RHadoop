#
# Filters through all the workingdir/*failed.log files, 
# and extracts number of packages depending on this failed package. 
#
# Usage:
#   From R Command line, Make sure current working directory is
#   in folder above the workingdir where all the log files are found. 
#  > source("logAnalyser.R")
#
# Derek Brown 2009/12/23 (Originally Alex Chen)
#
p <- system("grep 'also installing the dependenc' workingdir/*.failed.log", intern=T)
regexpr("installing the dependenc\\w+\\s+", p)->ip
attr(ip, "match.length")->np
substring(p, ip+np)->zp
print(as.data.frame(sort(table(unlist(strsplit(zp, ", "))))))
