#
# Filters through all the workingdir/*.log files, 
# and extracts list of dependencies that are not available. 
# 
# Usage:
#   From R Command line, Make sure current working directory is
#   in folder above the workingdir where all the log files are found. 
#  > source("logAnalyserExt.R")
#
# Derek Brown 2009/12/23 (Originally Alex Chen)
#
p <- system("grep '^Warning: dependenc' workingdir/*.*log", intern=T)
regexpr("Warning: dependenc\\w+\\s+", p)->ip
attr(ip, "match.length")->np
substring(p, ip+np)->zp
unlist(strsplit(zp, ", "))->zpp
regexpr("'[^']*'", zpp)->ip
attr(ip, "match.length")->np
substring(zpp, ip, ip+np)->zppp

print(as.data.frame(sort(table(zppp))))
