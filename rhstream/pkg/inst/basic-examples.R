rawtextinputformat = function(line) {keyval(NULL, line)}
             
mrwordcount = function (infile, outfile, pattern = " ") {
  revoMapReduce(in.folder = infile ,
           out.folder = outfile,
           textinputformat = rawtextinputformat,
           map = function(k,v) {
             lapply(
                    strsplit(
                             x = v,
                             split = pattern)[[1]],
                    function(w) keyval(w,1))},
           reduce = function(k,vv) {
             keyval(k, sum(unlist(vv)))})}

filtermap= function(pred) function(k,v) {if (pred(v)) keyval(k,v) else NULL}

mrfilter = function (infile, outfile, pred) {
  revoMapReduce(in.folder = infile ,
           out.folder = outfile,
           map = filtermap(pred))
         }
