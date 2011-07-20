## classic wordcount 
##input can be any text file
mrwordcount = function (input, output, pattern = " ") {
  revoMapReduce(input = input ,
           output = output,
           textinputformat = rawtextinputformat,
           map = function(k,v) {
             lapply(
                    strsplit(
                             x = v,
                             split = pattern)[[1]],
                    function(w) keyval(w,1))},
           reduce = function(k,vv) {
             keyval(k, sum(unlist(vv)))})}

##input can be any RevoStreaming file (our own format)
## pred can be function(x) x > 0
## it will be evaluated on the value only, not on the key
## test set: rhwrite (lapply (1:10, function(i) keyval(rnorm(2))), "/tmp/filtertest")
## run with mrfilter("/tmp/filtertest", "/tmp/filterout", function(x) x > 0)

filtermap= function(pred) function(k,v) {if (pred(v)) keyval(k,v) else NULL}

mrfilter = function (input, output, pred) {
  revoMapReduce(input = input,
           output = output,
           map = filtermap(pred))
}

## pipeline of two filters, sweet
# rhread(mrfilter(input = mrfilter(
#                  input = "/tmp/filtertest/",
#                  pred = function(x) x > 0),
#                pred = function(x) x < 0.5))
