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



#options

rmr.options.env = new.env(parent=emptyenv())
rmr.options.env$backend = "hadoop"
rmr.options.env$vectorized.keyval.length = 1000
rmr.options.env$profile.nodes = FALSE
rmr.options.env$depend.check = FALSE
#rmr.options$managed.dir = "/var/rmr/managed"

rmr.options = 
  function(backend = c("hadoop", "local"), 
           profile.nodes = FALSE,
           vectorized.keyval.length = 1000#, 
           #depend.check = FALSE, 
           #managed.dir = FALSE
           ) {
    args = as.list(sys.call())[-1]
    this.call = match.call()
    lapply(
      names(args),
      function(x) {
        if(x != "")
          assign(x, eval(this.call[[x]]), envir = rmr.options.env)})
    read.args =
      if(is.null(names(args)))
        args
    else 
      named.slice(args, "")
    if(length(read.args) > 0) {
      read.args = simplify2array(read.args)
      retval = as.list(rmr.options.env)[read.args]
      if (length(retval) == 1) retval[[1]] else retval}
    else NULL }

## map and reduce function generation

to.map = 
  function(fun1, fun2 = identity) {
    if (missing(fun2)) {
      function(k, v) fun1(keyval(k, v))}
    else {
      function(k, v) keyval(fun1(k), fun2(v))}}

to.reduce = to.map

to.reduce.all = 
  function(fun1, fun2 = identity) {
    if (missing(fun2)) {
      function(k, vv) lapply(vv, function(v) fun1(keyval(k, v)))}
    else {
      function(k, vv) lapply(vv, function(v) keyval(fun1(k), fun2(v)))}}

## mapred combinators

compose.mapred = 
  function(mapred, map) 
    function(k, v) {
      out = mapred(k, v)
      if (is.null(out)) NULL
      else map(keys(out), values(out))}
      
union.mapred = 
  function(mr1, mr2) function(k, v) {
    c.keyval(mr1(k, v), mr2(k, v))}

#output cmp
cmp = function(x, y) {
  kx  = keys(x)
  ky = keys(y)
  vx = values(x)
  vy = values(y)
  ox = order(sapply(kx, digest), sapply(vx, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
  oy = order(sapply(ky, digest), sapply(vy, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
  isTRUE(all.equal(kx[ox], ky[oy], check.attributes = FALSE)) &&
  isTRUE(all.equal(vx[ox], vy[oy], check.attributes = FALSE))}


# backend independent dfs section
part.list = function(fname) {
  if(rmr.options('backend') == "local") fname
  else {
    if(dfs.is.dir(fname))
      pretty.hdfs.ls(paste(fname, "part*", sep = "/"))$path
    else fname}}

dfs.exists = function(f) {
  if (rmr.options('backend') == 'hadoop') 
    hdfs.test(e = f) 
  else file.exists(f)}

dfs.rm = function(f) {
  if(rmr.options('backend') == 'hadoop')
    hdfs.rm(f)
  else file.remove(f)}

dfs.is.dir = function(f) { 
  if (rmr.options('backend') == 'hadoop') 
    hdfs.test(d = f)
  else file.info(f)['isdir']}

dfs.empty = function(f) {
  f = to.dfs.path(f)
  if(rmr.options('backend') == 'hadoop') {
    if(dfs.is.dir(f)) {
      all(
        lapply(
          part.list(f),
          function(x) hdfs.test(z = x)))}
    else {hdfs.test(z = f)}}
  else file.info(f)['size'] == 0}

# dfs bridge

to.dfs.path = function(input) {
  if (is.character(input)) {
    input}
  else {
    if(is.function(input)) {
      input()}}}

to.dfs = function(kv, output = dfs.tempfile(), format = "native") {
  kv = as.keyval(kv)
  tmp = tempfile()
  dfsOutput = to.dfs.path(output)
  if(is.character(format)) format = make.output.format(format)
  
  write.file = 
    function(kv, f) {
      con = file(f, if(format$mode == "text") "w" else "wb")
        keyval.writer = make.keyval.writer(format$mode, 
                                           format$format, 
                                           rmr.options('vectorized.keyval.length'),
                                           con)
      keyval.writer(kv)
      
      close(con)}
    
  write.file(kv, tmp)      
  if(rmr.options('backend') == 'hadoop') {
    if(format$mode == "binary")
      system(paste(hadoop.streaming(),  "loadtb", dfsOutput, "<", tmp))
    else  hdfs.put(tmp, dfsOutput)}
  else file.copy(tmp, dfsOutput)
  file.remove(tmp)
  output}

from.dfs = function(input, format = "native") {
  
  read.file = function(f) {
    con = file(f, if(format$mode == "text") "r" else "rb")
    keyval.reader = make.keyval.reader(format$mode, format$format, rmr.options('vectorized.keyval.length'), con)
    retval = make.fast.list()
    kv = keyval.reader()
    while(!is.null(kv)) {
      retval(list(kv))
      kv = keyval.reader()}
    close(con)
    c.keyval(retval())}
  
  dumptb = function(src, dest){
    lapply(src, function(x) system(paste(hadoop.streaming(), "dumptb", x, ">>", dest)))}
  
  getmerge = function(src, dest) {
    on.exit(unlink(tmp))
    tmp = tempfile()
    lapply(src, function(x) {
      hdfs.get(as.character(x), tmp)
      system(paste('cat', tmp, '>>' , dest))
      unlink(tmp)})
    dest}
  
  fname = to.dfs.path(input)
  if(is.character(format)) format = make.input.format(format)
  if(rmr.options("backend") == "hadoop") {
    tmp = tempfile()
    if(format$mode == "binary") dumptb(part.list(fname), tmp)
    else getmerge(part.list(fname), tmp)}
  else
    tmp = fname
  retval = read.file(tmp)
  if(rmr.options("backend") == "hadoop") unlink(tmp)
  retval}

# mapreduce

dfs.tempfile = function(pattern = "file", tmpdir = tempdir()) {
  fname  = tempfile(pattern, tmpdir)
  namefun = function() {fname}
  reg.finalizer(environment(namefun), 
                function(e) {
                  fname = eval(expression(fname), envir = e)
                  if(Sys.getenv("mapred_task_id") != "" && dfs.exists(fname)) dfs.rm(fname)
		})
  namefun
}

dfs.managed.file = function(call, managed.dir = rmr.options('managed.dir')) {
  file.path(managed.dir, digest(lapply(call, eval)))}

mapreduce = function(
  input, 
  output = NULL, 
  map = to.map(identity), 
  reduce = NULL, 
  combine = NULL, 
  input.format = "native", 
  output.format = "native", 
  backend.parameters = list(), 
  verbose = TRUE) {

  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) output = 
    if(rmr.options('depend.check'))
      dfs.managed.file(match.call())
    else
      dfs.tempfile()
  if(is.character(input.format)) input.format = make.input.format(input.format)
  if(is.character(output.format)) output.format = make.output.format(output.format)
  if(!missing(backend.parameters)) warning("backend.parameters is deprecated.")
  
  backend  =  rmr.options('backend')
 
  mr = switch(backend, 
              hadoop = rmr.stream, 
              local = mr.local, 
              stop("Unsupported backend: ", backend))
  
  mr(map = map, 
     reduce = reduce, 
     combine = combine, 
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input), 
     out.folder = to.dfs.path(output), 
     profile.nodes = rmr.options('profile.nodes'), 
     vectorized.keyval.length = rmr.options('vectorized.keyval.length'),
     input.format = input.format, 
     output.format = output.format, 
     backend.parameters = backend.parameters[[backend]], 
     verbose = verbose)
  output
}


##special jobs
 
## a sort of relational join very useful in a variety of map reduce algorithms

## to.dfs(lapply(1:10, function(i) keyval(i, i^2)), "/tmp/reljoin.left")
## to.dfs(lapply(1:10, function(i) keyval(i, i^3)), "/tmp/reljoin.right")
## equijoin(left.input="/tmp/reljoin.left", right.input="/tmp/reljoin.right", output = "/tmp/reljoin.out")
## from.dfs("/tmp/reljoin.out")

equijoin = function(
  left.input = NULL, 
  right.input = NULL, 
  input = NULL, 
  output = NULL, 
  outer = c("", "left", "right", "full"), 
  map.left = to.map(identity), 
  map.right = to.map(identity), 
  reduce  = function(k, values.left, values.right) keyval(k, merge(values.left, values.right, by = NULL)), 
  reduce.all  = function(k, vl, vr) keyval(k, list(left = vl, right = vr))) { 
  
  stopifnot(xor(!is.null(left.input), !is.null(input) &&
                (is.null(left.input) == is.null(right.input))))
  outer = match.arg(outer)
  left.outer = outer == "left"
  right.outer = outer == "right"
  full.outer = outer == "full"
  if (is.null(left.input)) {
    left.input = input}
  mark.side =
    function(kv, is.left) {
      kv = split.keyval(kv)
      keyval(keys(kv),
             lapply(values(kv),
                    function(v) {
                      attributes(v) = c(list(is.left = is.left), attributes(v))
                      v}))}
  is.left.side = 
    function(left.input) {
      leftin = strsplit(to.dfs.path(left.input), "/+")[[1]]
      mapin = strsplit(Sys.getenv("map_input_file"), "/+")[[1]]
      leftin = leftin[-1]
      mapin = mapin[if(is.element(mapin[1], c("hdfs:", "maprfs:"))) c(-1, -2) else -1]
      all(mapin[1:length(leftin)] == leftin)}
  reduce.split =
    function(vv) {
      tapply(vv, 
             sapply(vv, function(v) attr(v, "is.left", exact=T)), 
             identity, 
             simplify = FALSE)}
  pad.side =
    function(vv, side.outer, full.outer) if (length(vv) == 0 && (side.outer || full.outer)) c(NA) else vv
  map = if (is.null(input)) {
    function(k, v) {
      ils = switch(rmr.options('backend'), 
                   hadoop = is.left.side(left.input), 
                   local = attr(v, 'rmr.input') == to.dfs.path(left.input), 
                   stop("Unsupported backend: ", rmr.options('backend')))
      mark.side(if(ils) map.left(k, v) else map.right(k, v), ils)}}
  else {
    function(k, v) {
      c.keyval(mark.side(map.left(k, v), TRUE), 
               mark.side(map.right(k, v), FALSE))}}
  eqj.reduce = reduce
  mapreduce(map = map, 
            reduce =
            function(k, vv) {
              rs = reduce.split(vv)
              eqj.reduce(c.or.rbind(k), 
                     pad.side(c.or.rbind(rs$`TRUE`), right.outer, full.outer), 
                     pad.side(c.or.rbind(rs$`FALSE`), left.outer, full.outer))}, 
            input = c(left.input, right.input), 
            output = output)}
