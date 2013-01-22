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
rmr.options.env$keyval.length = 10000
rmr.options.env$profile.nodes = "off"
rmr.options.env$depend.check = FALSE
rmr.options.env$install.args = NULL
rmr.options.env$update.args = NULL
#rmr.options$managed.dir = "/var/rmr/managed"

rmr.options = 
  function(backend = c("hadoop", "local"), 
           profile.nodes = c("off", "calls", "memory", "both"),
           keyval.length = 1000,
           install.args = NULL,
           update.args = NULL#, 
           #depend.check = FALSE, 
           #managed.dir = FALSE
  ) {
    args = as.list(sys.call())[-1]
    this.call = match.call()
    if (is.logical(profile.nodes)) {
      this.call[["profile.nodes"]] = {
        if(profile.nodes)
          "calls"
        else
          "off"}}
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
cmp = 
  function(x, y) {
    kx  = keys(x)
    ky = keys(y)
    vx = values(x)
    vy = values(y)
    ox = order(sapply(kx, digest), sapply(vx, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
    oy = order(sapply(ky, digest), sapply(vy, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
    isTRUE(all.equal(kx[ox], ky[oy], check.attributes = FALSE)) &&
      isTRUE(all.equal(vx[ox], vy[oy], check.attributes = FALSE))}

# backend independent dfs section

is.hidden.file = 
  function(f)
    regexpr("[\\._]", basename(f)) == 1

part.list = 
  function(fname) {
    if(rmr.options('backend') == "local") fname
    else {
      if(dfs.is.dir(fname)) {
        du = hdfs.du(fname)
        du[!is.hidden.file(du[,2]),2]}
        else fname}}

dfs.exists = 
  function(f) {
    if (rmr.options('backend') == 'hadoop') 
      hdfs.test(e = f) 
    else file.exists(f)}

dfs.rm = 
  function(f) {
    if(rmr.options('backend') == 'hadoop')
      hdfs.rm(f)
    else file.remove(f)}

dfs.is.dir = 
  function(f) { 
    if (rmr.options('backend') == 'hadoop') 
      hdfs.test(d = f)
    else file.info(f)['isdir']}

dfs.empty = 
  function(f) 
    dfs.size(f) == 0

dfs.size = 
  function(f) {
    f = to.dfs.path(f)
    if(rmr.options('backend') == 'hadoop') {
      du = hdfs.du(f)
      if(is.null(du)) 0 
      else
        sum(as.integer(du[!is.hidden.file(du[,2]), 1]))}
    else file.info(f)[1, 'size'] }

# dfs bridge

to.dfs.path = 
  function(input) {
    if (is.character(input)) {
      input}
    else {
      if(is.function(input)) {
        input()}}}

to.dfs = 
  function(
    kv, 
    output = dfs.tempfile(), 
    format = "native") {
    if(!is.keyval(kv))
      warning("Converting to.dfs argument to keyval with a NULL key")
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
    keyval.reader = make.keyval.reader(format$mode, format$format, rmr.options('keyval.length'), con)
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
	if(.Platform$OS.type == "windows") {
	  cmd = paste('type', tmp, '>>' , dest)
	  system(paste(Sys.getenv("COMSPEC"),"/c",cmd))
	}
	else {
	  system(paste('cat', tmp, '>>' , dest))
        }
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
  subfname = strsplit(fname, ":")
  if(length(subfname[[1]]) > 1) fname = subfname[[1]][2]
  namefun = function() {fname}
  reg.finalizer(environment(namefun), 
                function(e) {
                  fname = eval(expression(fname), envir = e)
                  if(Sys.getenv("mapred_task_id") != "" && dfs.exists(fname)) dfs.rm(fname)
                },
                onexit = TRUE)
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
  in.mem.combine = FALSE,
  backend.parameters = list(), 
  verbose = TRUE) {
  
  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) 
    output = {
      if(rmr.options('depend.check'))
        dfs.managed.file(match.call())
      else
        dfs.tempfile()}
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
     keyval.length = rmr.options('keyval.length'),
     rmr.install = {
       if(!is.null(rmr.options('install.args')))
         do.call(Curry, c(install.packages,rmr.options('install.args')))
       else NULL},
     rmr.update = {
       if(!is.null(rmr.options('update.args')))
         do.call(Curry, c(update.packages, rmr.options('update.args')))
       else NULL}, 
     input.format = input.format, 
     output.format = output.format, 
     in.mem.combine = in.mem.combine,
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

reduce.default = 
  function(k, vl, vr) {
    if((is.list(vl) && !is.data.frame(vl)) || 
         (is.list(vr) && !is.data.frame(vr)))
      list(left = vl, right = vr)
    else{
      if(!is.null(names(vl)))
        names(vl) = paste(names(vl), "l", sep = ".")
      if(!is.null(names(vr)))
        names(vr) = paste(names(vr), "r", sep = ".")
      if(all(is.na(vl))) vr
      else {
        if(all(is.na(vr))) vl
        else
          merge(vl, vr, by = NULL)}}}

equijoin = 
  function(
    left.input = NULL, 
    right.input = NULL, 
    input = NULL, 
    output = NULL, 
    outer = c("", "left", "right", "full"), 
    map.left = to.map(identity), 
    map.right = to.map(identity), 
    reduce  = reduce.default) { 
    
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
                      list(val = v, is.left = is.left)}))}
  rmr.normalize.path = 
    function(url.or.path) {
      if(.Platform$OS.type == "windows")
        url.or.path = gsub("\\\\","/", url.or.path)
      gsub(
        "/+", 
        "/", 
        paste(
          "/", 
          gsub(
            "part-[0-9]+$", 
            "", 
            parse_url(url.or.path)$path), 
          "/", 
          sep = ""))}
  is.left.side = 
    function(left.input) {
      rmr.normalize.path(to.dfs.path(left.input)) ==
        rmr.normalize.path(Sys.getenv("map_input_file"))}
  reduce.split =
    function(vv) {
      tapply(
        vv, 
        sapply(vv, function(v) v$is.left), 
        function(v) lapply(v, function(x)x$val), 
        simplify = FALSE)}
  pad.side =
    function(vv, side.outer, full.outer) 
      if (length(vv) == 0 && (side.outer || full.outer)) c(NA) else c.or.rbind(vv)
  map = 
    if (is.null(input)) {
      function(k, v) {
        ils = is.left.side(left.input)
        mark.side(if(ils) map.left(k, v) else map.right(k, v), ils)}}
  else {
    function(k, v) {
      c.keyval(mark.side(map.left(k, v), TRUE), 
               mark.side(map.right(k, v), FALSE))}}
  eqj.reduce = 
    function(k, vv) {
      rs = reduce.split(vv)
      left.side = pad.side(rs$`TRUE`, right.outer, full.outer)
      right.side = pad.side(rs$`FALSE`, left.outer, full.outer)
      if(!is.null(left.side) && !is.null(right.side))
        reduce(k[[1]], left.side, right.side)}
  mapreduce(
    map = map, 
    reduce = eqj.reduce,
    input = c(left.input, right.input), 
    output = output)}

status = function(value)
  cat(
    sprintf("reporter:status:%s\n", 
            value), 
    file = stderr())

increment.counter =
  function(group, counter, increment = 1)
      cat(
        sprintf(
          "reporter:counter:%s\n", 
          paste(group, counter, increment, sep=",")), 
        file = stderr())
