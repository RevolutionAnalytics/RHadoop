##refresh = function(f, args) {
  ##calculate output fname
  ## if out missing recalculate
  ## else
  ## get date of output
  ## get date of file input(s)
  ## if any inputs newer than output recalculate
  ## else return output fname
##}

#additional logic for revoMR
## revoMapReduce(input, output = hdfs.tempfile(), ...) {
##   if(!is.character(outtype)) {
##     outattr = attr(output, 'property')
##     if(outattr = 'tempfile') {
##       output = output()
##     }
##     else{## make mode
##       outfname = digest(args)
##       if (older(input, output)) return output
##     }
##   }
##   rhstream(input, output, ...)


           
##if output is missing or tempfile, write to tempfile, recompute
##if output is pseudomake, write to digest(args) if older than input
##if output is path, write to path, recompute



