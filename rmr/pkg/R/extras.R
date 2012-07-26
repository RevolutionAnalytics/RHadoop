## push a file through this to get as many partitions as possible (depending on system settings)
## data is unchanged

scatter = function(input, output = NULL)
  mapreduce(input, 
            output, 
            map = function(k, v) keyval(sample(1:1000, 1), keyval(k, v)), 
            reduce = function(k, vv) vv)

##optimizer

is.mapreduce = function(x) {
  is.call(x) && x[[1]] == "mapreduce"}

mapreduce.arg = function(x, arg) {
  match.call(mapreduce, x) [[arg]]}

optimize = function(mrex) {
  mrin = mapreduce.arg(mrex, 'input')
  if (is.mapreduce(mrex) && 
    is.mapreduce(mrin) &&
    is.null(mapreduce.arg(mrin, 'output')) &&
    is.null(mapreduce.arg(mrin, 'reduce'))) {
    bquote(
      mapreduce(input =  .(mapreduce.arg(mrin, 'input')), 
                output = .(mapreduce.arg(mrex, 'output')), 
                map = .(compose.mapred)(.(mapreduce.arg(mrex, 'map')), 
                                        .(mapreduce.arg(mrin, 'map'))), 
                reduce = .(mapreduce.arg(mrex, 'reduce'))))}
  else mrex }


##other

reload = function() {
  detach("package:rmr", unload=T)
  library.dynam.unload("rmr",system.file(package="rmr"))
  library(rmr)}
