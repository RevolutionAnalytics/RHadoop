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

 
## a sort of relational join very useful in a variety of map reduce algorithms

## to.dfs(lapply(1:10, function(i) keyval(i, i^2)), "/tmp/reljoin.left")
## to.dfs(lapply(1:10, function(i) keyval(i, i^3)), "/tmp/reljoin.right")
## relational.join(leftinput="/tmp/reljoin.left", rightinput="/tmp/reljoin.right", output = "/tmp/reljoin.out")
## from.dfs("/tmp/reljoin.out")

relational.join = function(
  leftinput = NULL,
  rightinput = NULL,
  input = NULL,
  output = NULL,
  outer = "",
  map.left = to.map(identity),
  map.right = to.map(identity),
  reduce  = function (k, vl, vr) keyval(k, list(left=vl, right=vr)))
{
  stopifnot(xor(!is.null(leftinput), !is.null(input) &&
                (is.null(leftinput)==is.null(rightinput))))
  stopifnot(is.element(outer, c("", "left", "right", "full")))
  leftouter = outer == "left"
  rightouter = outer == "right"
  fullouter = outer == "full"
  if (is.null(leftinput)) {
    leftinput = input}
  markSide =
    function(kv, isleft) keyval(kv$key, list(val = kv$val, isleft = isleft))
  isLeftSide = 
    function(leftinput) {
      leftinput = sub("//", "/", rmr:::to.hdfs.path(leftinput))
      mapInfile = sub("//", "/", Sys.getenv("map_input_file"))
      leftinput == substr(mapInfile, 6, 5 + nchar(leftinput))}
  reduce.split =
    function(vv) tapply(lapply(vv, function(v) v$val), sapply(vv, function(v) v$isleft), identity, simplify = FALSE)
  padSide =
    function(vv, sideouter, fullouter) if (length(vv) == 0 && (sideouter || fullouter)) c(NA) else vv
  map = if (is.null(input)) {
    function(k,v) {
      ils = isLeftSide(leftinput)
      markSide(if(ils) map.left(k,v) else map.right(k,v), ils)
    }
  }
  else {
    function(k,v) {
      list(markSide(map.left(k,v), TRUE),
           markSide(map.right(k,v), FALSE))
    }
  }
  mapreduce(map = map,
            reduce =
            function(k, vv) {
              rs = reduce.split(vv)
              values.left = padSide(rs$`TRUE`, rightouter, fullouter)
              values.right = padSide(rs$`FALSE`, leftouter, fullouter)
              do.call(c,
                      lapply(values.left,
                             function(x) lapply(values.right,
                                                function(y) reduce(k, x, y))))},
                ## combiner = F,
            input = c(leftinput,rightinput),
            output = output)}

