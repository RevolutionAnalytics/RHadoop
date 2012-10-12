


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
      map = function(k,v) keyval(k,v))
```

## 
<ul class="incremental" style="list-style: none" >
<li>

```r
  predicate = 
    function(.,v) unlist(v)%%2 == 0
```

<li> 

```r
    mapreduce(
      input, 
      map = 
        function(k,v) {
          filter = predicate(k,v); 
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
              map = function(.,v) v$b)
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
        function(.,v) keyval(1, sum(v)), 
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
  group = function(k,v) k%%100
  aggregate = function(x) sum(x)
```

## 

```r
    mapreduce(
      input.ga, 
      map = 
        function(k,v) 
          keyval(group(k,v), v),
      reduce = 
        function(k, vv) 
          keyval(k, aggregate(vv)),
      combine = TRUE)
```

