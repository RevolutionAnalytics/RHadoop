# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## push a file through this to get as many partitions as possible (depending on system settings)
## data is unchanged

scatter = function(input, output = NULL, ...)
  mapreduce(input, 
            output, 
            map = function(k, v) keyval(sample(1:1000, size = rmr2:::rmr.length(v), replace = TRUE), v), 
            reduce = function(k, vv) vv)

gather = function(input, output = NULL, ...) {
  backend.parameters = list(...)['backend.prameters']
  backend.parameters$hadoop = append(backend.parameters$hadoop, list(D='mapred.reduce.tasks=1'))
  mapreduce(input,
            output, 
            backend.parameters = backend.parameters,
            ...)}
            
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

rmr.sample = function(input, output = NULL, method = c("any", "Bernoulli"), ...) {
  method = match.arg(method)
  if (method == "any") {
    map.n = list(...)[['n']]
    reduce.n = map.n
    mapreduce(input, 
              output,
              map = function(k,v) {
                if (map.n > 0){
                  map.n <<- map.n - 1
                  keyval(1, keyval(k,v))}},
              combine = function(k, vv) {
                lapply(vv[1:min(reduce.n, length(vv))], function(v) keyval(NULL,v))},
              reduce = function(k, vv) 
                vv[1:min(reduce.n, length(vv))])}
  else
    if(method == "Bernoulli"){
      p = list(...)[['p']]
      mapreduce(input,
                output,
                map = function(k,v)
                  if(rbinom(1,1,p) == 1)
                    keyval(k,v))}}

## dev support

reload = function() {
  detach("package:rmr2", unload=T)
  library.dynam.unload("rmr2",system.file(package="rmr2"))
  library(rmr2)}

rmr.print  = function(x) {
  message(paste(match.call() [[2]], paste(capture.output(str(x)), collapse="\n")))}


