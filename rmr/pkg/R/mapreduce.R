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

rmr.options = new.env(parent=emptyenv())
rmr.options$backend = "hadoop"
rmr.options$vectorized.keyval.length = 1000
rmr.options$profile.nodes = FALSE
rmr.options$depend.check = FALSE
#rmr.options$managed.dir = "/var/rmr/managed"

rmr.options.get = function(...) {
  opts = as.list(rmr.options)
  if(missing(...))
    opts
  else {
    args = c(...)
    if (length(args) > 1)
      opts[args]
    else 
      opts[[args]]}}

rmr.options.set = function(backend = c("hadoop", "local"), 
                           profile.nodes = NULL,
                           vectorized.keyval.length = NULL#, 
                           #depend.check = NULL, 
                           #managed.dir = NULL
                           ) {
  this.call = match.call()
  backend = match.arg(backend) #this doesn't do anything, fix
  lapply(names(this.call)[-1], 
         function(x) 
           assign(x, eval(this.call[[x]]), envir = rmr.options))
  as.list(rmr.options)}


## could think of this as a utils section

to.list = function(x) {
  if(is.data.frame(x)) {
    .Call('dataframe_to_list', x, nrow(x), ncol(x), rep(as.list(1:ncol(x)), nrow(x)))}
    else {
      if(is.matrix(x))
        apply(x,1,as.list)
      else
        as.list(x)}}


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
  
#some option formatting utils

paste.options = function(optlist) {
  optlist = unlist(sapply(optlist, function(x) if (is.logical(x)) {if(x) "" else NULL} else x))
  if(is.null(optlist)) "" 
  else paste(unlist(rbind(paste("-", names(optlist), sep = ""), optlist)), collapse = " ")}

make.input.files = function(infiles) {
  if(length(infiles) == 0) return(" ")
  paste(sapply(infiles, 
               function(r) {
                 sprintf("-input %s ", r)}), 
        collapse=" ")}


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

#hdfs section

hdfs = function(cmd, intern, ...) {
  if (is.null(names(list(...)))) {
    argnames = sapply(1:length(list(...)), function(i) "")}
  else {
    argnames = names(list(...))}
  system(paste(hadoop.cmd(), " dfs -", cmd, " ", 
              paste(
                apply(cbind(argnames, list(...)), 1, 
                  function(x) paste(
                    if(x[[1]] == "") {""} else {"-"}, 
                    x[[1]], 
                    " ", 
                    to.dfs.path(x[[2]]), 
                    sep = ""))[
                      order(argnames, decreasing = T)], 
                collapse = " "), 
               sep = ""), 
         intern = intern)}

getcmd = function(matched.call)
  strsplit(tail(as.character(as.list(matched.call)[[1]]), 1), "\\.")[[1]][[2]]
           
hdfs.match.sideeffect = function(...) {
  hdfs(getcmd(match.call()), FALSE, ...) == 0}

#this returns a character matrix, individual cmds may benefit from additional transformations
hdfs.match.out = function(...) {
  oldwarn = options("warn")[[1]]
  options(warn = -1)
  retval = do.call(rbind, strsplit(hdfs(getcmd(match.call()), TRUE, ...), " +")) 
  options(warn = oldwarn)
  retval}

mkhdfsfun = function(hdfscmd, out)
  eval(parse(text = paste ("hdfs.", hdfscmd, " = hdfs.match.", if(out) "out" else "sideeffect", sep = "")), 
       envir = parent.env(environment()))

for (hdfscmd in c("ls", "lsr", "df", "du", "dus", "count", "cat", "text", "stat", "tail", "help")) 
  mkhdfsfun(hdfscmd, TRUE)

for (hdfscmd in c("mv", "cp", "rm", "rmr", "expunge", "put", "copyFromLocal", "moveFromLocal", "get", "getmerge", 
                 "copyToLocal", "moveToLocal", "mkdir", "setrep", "touchz", "test", "chmod", "chown", "chgrp"))
  mkhdfsfun(hdfscmd, FALSE)

pretty.hdfs.ls = function(...) {
  ls.out = hdfs.ls(...)
  crud = grep("Found", ls.out[,1])
  if(length(crud) > 0)
    ls.out = ls.out[-crud,]
  if(class(ls.out) == "character") ls.out = t(ls.out)
  df = as.data.frame(ls.out,stringsAsFactors=F)
  names(df) = c("mode", "links", "owner", "group", "size", "last.modified.date", "last.modified.time", "path")
  df$links = as.numeric(sapply(as.character(df$links), function(x) if (x=="-") 0 else x))
  df$size = as.numeric(as.character(df$size))
  df}

# backend independent dfs section
part.list = function(fname) {
  if(rmr.options.get('backend') == "local") fname
  else {
    if(dfs.is.dir(fname))
      pretty.hdfs.ls(paste(fname, "part*", sep = "/"))$path
    else fname}}

dfs.exists = function(f) {
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(e = f) 
  else file.exists(f)}

dfs.rm = function(f) {
  if(rmr.options.get('backend') == 'hadoop')
    hdfs.rm(f)
  else file.remove(f)}

dfs.is.dir = function(f) { 
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(d = f)
  else file.info(f)['isdir']}

dfs.empty = function(f) {
  f = to.dfs.path(f)
  if(rmr.options.get('backend') == 'hadoop') {
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
                                           con)
      keyval.writer(kv)
      
      close(con)}
    
  write.file(kv, tmp)      
  if(rmr.options.get('backend') == 'hadoop') {
    if(format$mode == "binary")
      system(paste(hadoop.streaming(),  "loadtb", dfsOutput, "<", tmp))
    else  hdfs.put(tmp, dfsOutput)}
  else file.copy(tmp, dfsOutput)
  file.remove(tmp)
  output}

from.dfs = function(input, format = "native") {
  
  read.file = function(f) {
    con = file(f, if(format$mode == "text") "r" else "rb")
    keyval.reader = make.keyval.reader(format$mode, format$format, con)
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
  if(rmr.options.get("backend") == "hadoop") {
    tmp = tempfile()
    if(format$mode == "binary") dumptb(part.list(fname), tmp)
    else getmerge(part.list(fname), tmp)}
  else
    tmp = fname
  retval = read.file(tmp)
  if(rmr.options.get("backend") == "hadoop") unlink(tmp)
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

dfs.managed.file = function(call, managed.dir = rmr.options.get('managed.dir')) {
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
    if(rmr.options.get('depend.check'))
      dfs.managed.file(match.call())
    else
      dfs.tempfile()
  if(is.character(input.format)) input.format = make.input.format(input.format)
  if(is.character(output.format)) output.format = make.output.format(output.format)
  if(!missing(backend.parameters)) warning("backend.parameters is deprecated.")
  
  backend  =  rmr.options.get('backend')
  profile.nodes = rmr.options.get('profile.nodes')
    
  mr = switch(backend, 
              hadoop = rhstream, 
              local = mr.local, 
              stop("Unsupported backend: ", backend))
  
  mr(map = map, 
     reduce = reduce, 
     combine = combine, 
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input), 
     out.folder = to.dfs.path(output), 
     profile.nodes = profile.nodes, 
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
  reduce  = function(k, values.left, values.right)
    do.call(c, 
            lapply(values.left, 
                   function(vl) lapply(values.right, 
                                       function(vr) reduce.all(k, vl, vr)))), 
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
      rmr.print(vv)
      tapply(vv, 
             sapply(vv, function(v) attr(v, "is.left", exact=T)), 
             identity, 
             simplify = FALSE)}
  pad.side =
    function(vv, side.outer, full.outer) if (length(vv) == 0 && (side.outer || full.outer)) c(NA) else vv
  map = if (is.null(input)) {
    function(k, v) {
      ils = switch(rmr.options.get('backend'), 
                   hadoop = is.left.side(left.input), 
                   local = attr(v, 'rmr.input') == to.dfs.path(left.input), 
                   stop("Unsupported backend: ", rmr.options.get('backend')))
      mark.side(if(ils) map.left(k, v) else map.right(k, v), ils)}}
  else {
    function(k, v) {
      z = c.keyval(mark.side(map.left(k, v), TRUE), 
           mark.side(map.right(k, v), FALSE))
      rmr.print(z)
      z}}
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
