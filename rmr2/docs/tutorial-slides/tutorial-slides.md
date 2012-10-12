








## RHadoop Tutorial
### Revolution Analytics
#### Antonio Piccolboni
#### rhadoop@revolutionanalytics.com
#### antonio@piccolboni.info

#RHadoop 

##

- R + Hadoop
- OSS
- <img src="revolution.jpeg" width="20%">
- <img src="rhadoop.png" width="20%">
- rhdfs
- rhbase
- rmr2

# Mapreduce 

##

<ul class="incremental" style="list-style: none" >
<li>

```r
  small.ints = 1:1000
  sapply(small.ints, function(x) x^2)
```

<li>

```r
  small.ints = to.dfs(1:1000)
  mapreduce(
    input = small.ints, 
    map = function(k, v) cbind(v, v^2))
```

</ul>

## 

<ul class="incremental" style="list-style: none" >
<li>

```r
  groups = rbinom(32, n = 50, prob = 0.4)
  tapply(groups, groups, length)
```

<li>

```r
  groups = to.dfs(groups)
  from.dfs(
    mapreduce(
      input = groups, 
      map = function(., v) keyval(v, 1), 
      reduce = 
        function(k, vv) 
          keyval(k, length(vv))))
```

</ul>

# rmr-ABC

## 

<ul class="incremental" style="list-style: none" >
<li>

```r
    input = to.dfs(1:input.size)
```

<li>

```r
    from.dfs(input)
```

</ul>

## 

```r
    mapreduce(
      input, 
      map = function(k, v) keyval(k, v))
```

## 
<ul class="incremental" style="list-style: none" >
<li>

```r
  predicate = 
    function(., v) unlist(v)%%2 == 0
```

<li> 

```r
    mapreduce(
      input, 
      map = 
        function(k, v) {
          filter = predicate(k, v); 
          keyval(k[filter], v[filter])})
```

</ul>
## 
<ul class="incremental" style="list-style: none" >
<li>

```r
  input.select = 
    to.dfs(
      data.frame(
        a = rnorm(input.size),
        b = 1:input.size,
        c = sample(as.character(1:10),
                   input.size, 
                   replace=TRUE)))
```

<li>

```r
    mapreduce(input.select,
              map = function(., v) v$b)
```

</ul>
## 
<ul class="incremental" style="list-style: none" >
<li>

```r
  set.seed(0)
  big.sample = rnorm(input.size)
  input.bigsum = to.dfs(big.sample)
```

<li>

```r
    mapreduce(
      input.bigsum, 
      map  = 
        function(., v) keyval(1, sum(v)), 
      reduce = 
        function(., v) keyval(1, sum(v)),
      combine = TRUE)
```

</ul>
## 

```r
  input.ga = 
    to.dfs(
      keyval(
        1:input.size,
        rnorm(input.size)))
```

## 

```r
  group = function(k, v) k%%100
  aggregate = function(x) sum(x)
```

## 

```r
    mapreduce(
      input.ga, 
      map = 
        function(k, v) 
          keyval(group(k, v), v),
      reduce = 
        function(k, vv) 
          keyval(k, aggregate(vv)),
      combine = TRUE)
```


# Wordcount
##

<ul class="incremental" style="list-style: none" >
<li>

```r
wordcount = 
  function(
    input, 
    output = NULL, 
    pattern = " "){
```

<li>

```r
    mapreduce(input = input ,
              output = output,
              input.format = "text",
              map = wc.map,
              reduce = wc.reduce,
              combine = T)}
```

</ul>

##

<ul class="incremental" style="list-style: none" >
<li>

```r
    wc.map = 
      function(., lines) {
        keyval(
          unlist(
            strsplit(
              x = lines,
              split = pattern)),
          1)}
```

<li>

```r
    wc.reduce =
      function(word, counts ) {
        keyval(word, sum(counts))}
```

</ul>



# Logistic Regression

##
<ul class="incremental" style="list-style: none" >

<li>

```r
logistic.regression = 
  function(input, iterations, dims, alpha){
```

<li>

```r
  plane = t(rep(0, dims))
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = 
      values(
        from.dfs(
          mapreduce(
            input,
            map = lr.map,     
            reduce = lr.reduce,
            combine = T)))
    plane = plane + alpha * gradient }
  plane }
```

</ul>

##
<ul class="incremental" style="list-style: none" >

<li>

```r
  lr.map =          
    function(., M) {
      Y = M[,1] 
      X = M[,-1]
      keyval(
        1,
        Y * X * 
          g(-Y * as.numeric(X %*% t(plane))))}
```

</li>

```r
  lr.reduce =
    function(k, Z) 
      keyval(k, t(as.matrix(apply(Z,2,sum))))
```

</ul>

##

<ul class="incremental" style="list-style: none" >

<li>

```r
  eps = rnorm(test.size)
  testdata = 
    to.dfs(
      as.matrix(
        data.frame(
          y = 2 * (eps > 0) - 1,
          x1 = 1:test.size, 
          x2 = 1:test.size + eps)))
```

</li>

```r
    logistic.regression(
      testdata, 3, 2, 0.05)
```

</ul>


# K-means

##


```r
    dist.fun = 
      function(C, P) {
        apply(C,
              1, 
              function(x) 
                matrix(
                  rowSums((t(t(P) - x))^2), 
                  ncol = length(x)))}
```

 
##

```r
    kmeans.map.1 = 
      function(., P) {
        nearest = 
          if(is.null(C)) {
            sample(
              1:num.clusters, 
              nrow(P), 
              replace = T)}
          else {
            D = dist.fun(C, P)
            nearest = max.col(-D)}
        keyval(nearest, P) }
```


##

```r
    kmeans.reduce.1 = 
      function(x, P) {
        t(as.matrix(apply(P, 2, mean)))}
```


##

```r
kmeans.mr = 
  function(P, num.clusters, num.iter) {
```

##

```r
    C = NULL
    for(i in 1:num.iter ) {
      C = 
        values(
          from.dfs(
            mapreduce(
              P, 
              map = kmeans.map.1, 
              reduce = kmeans.reduce.1)))
      if(nrow(C) < 5) 
        C = 
          matrix(
            rnorm(
              num.clusters * nrow(C)), 
            ncol = nrow(C)) %*% C }
    C}
```


##
<ul class="incremental" style="list-style: none" >
<li>

```r
  input = 
    do.call(
      rbind, 
      rep(
        list(
          matrix(
            rnorm(10, sd = 10), 
            ncol=2)), 
        20)) + 
    matrix(rnorm(200), ncol =2)
```

<li>

```r
    kmeans.mr(
      to.dfs(input),
      num.clusters  = 12, 
      num.iter= 5)
```

</ul>

# Linear Least Squares

##
  
  $$ \mathbf{X b = y}$$  

```
  solve(t(X)%*%X, t(X)%*%y)
```
  
##


```r
Sum = 
  function(., YY) 
    keyval(1, list(Reduce('+', YY)))
```


##

```r
XtX = 
  values(
    from.dfs(
      mapreduce(
        input = X,
        map = 
          function(., Xi) 
            keyval(1, list(t(Xi) %*% Xi)),
        reduce = Sum,
        combine = TRUE)))[[1]]
```


##

```r
Xty = 
  values(
    from.dfs(
      mapreduce(
        input = X,
        map = function(., Xi)
          keyval(1, list(t(Xi) %*% y)),
        reduce = Sum,
        combine = TRUE)))[[1]]
```


##
<ul class="incremental" style="list-style: none" >

<li>

```r
solve(XtX, Xty)
```

<li>   


```r
X = to.dfs(matrix(rnorm(2000), ncol = 10))
y = as.matrix(rnorm(200))
```

</ul>
