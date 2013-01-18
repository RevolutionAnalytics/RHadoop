








## Scalable Analytics in R with rmr
### Revolution Analytics
##### Antonio Piccolboni


# RHadoop



# <img src="../resources/R.png">

## <img src="https://r4stats.files.wordpress.com/2012/04/fig_10_cran.png" width=75%>

<details>[r4stats](http://r4stats.com/popularity)
</details>

## <img src="../resources/revo-home.png" width=75%>

## <img src="../resources/rhadoop.png" width=50%>

<details>
hadoop brings horizontal scalability --
r sophisticated analytics --
combination could be powerful  
</details>

##

* rhdfs
* rhbase
* <em>rmr2</em>

<details>
why RHadoop for the R dev --
The data is in Hadoop --
The biggest cluster is Hadoop
</details>

##

```r
    library(rmr2)
  
    mapreduce(input, ...)
```

<details>
Just a library
Not a special run-time
Not a different language
Not a special purpose language
Incrementally port your code
Use all packages
</details>

##

```r
    sapply(data, function)

    mapreduce(big.data, map = function)
```

<details>
Very R-like, building on the functional characteristics of R
Upholds scope rules
</details>


## 
<table width=75% align=center>
<thead>
<th>Direct MR</th><th>Indirect MR</th>
</thead> 
<tr><td>&nbsp;</td></tr>
<tr>
<td></td><td><em>Hive, Pig</em></td>
</tr>
<tr><td>&nbsp;</td></tr>
<tr>
<td><strong>Rmr</strong>, Rhipe, Dumbo, Pydoop, Hadoopy</td><td>Cascalog, Scalding, Scrunch</td>
</tr>
<tr><td>&nbsp;</td></tr>
<tr> 
<td>Java, C++</td><td>Cascading, Crunch</td>
</tr>
</table>

<details>
Much simpler than writing Java
Not as simple as Hive, Pig at what they do, but more general, a real language
Great for prototyping, can transition to production -- optimize instead of rewriting! Lower risk, always executable.
</details>

##

```{.python style="font-size:12px"}
#!/usr/bin/python
import sys
from math import fabs
from org.apache.pig.scripting import Pig

filename = "student.txt"
k = 4
tolerance = 0.01

MAX_SCORE = 4
MIN_SCORE = 0
MAX_ITERATION = 100

# initial centroid, equally divide the space
initial_centroids = ""
last_centroids = [None] * k
for i in range(k):
  last_centroids[i] = MIN_SCORE + float(i)/k*(MAX_SCORE-MIN_SCORE)
  initial_centroids = initial_centroids + str(last_centroids[i])
  if i!=k-1:
    initial_centroids = initial_centroids + ":"

P = Pig.compile("""register udf.jar
          DEFINE find_centroid FindCentroid('$centroids');
          raw = load 'student.txt' as (name:chararray, age:int, gpa:double);
          centroided = foreach raw generate gpa, find_centroid(gpa) as centroid;
          grouped = group centroided by centroid;
          result = foreach grouped generate group, AVG(centroided.gpa);
          store result into 'output';
        """)

converged = False
iter_num = 0
while iter_num<MAX_ITERATION:
  Q = P.bind({'centroids':initial_centroids})
  results = Q.runSingle()
```


##

```{.python style="font-size:12px"}
  if results.isSuccessful() == "FAILED":
    raise "Pig job failed"
  iter = results.result("result").iterator()
  centroids = [None] * k
  distance_move = 0
  # get new centroid of this iteration, caculate the moving distance with last iteration
  for i in range(k):
    tuple = iter.next()
    centroids[i] = float(str(tuple.get(1)))
    distance_move = distance_move + fabs(last_centroids[i]-centroids[i])
  distance_move = distance_move / k;
  Pig.fs("rmr output")
  print("iteration " + str(iter_num))
  print("average distance moved: " + str(distance_move))
  if distance_move<tolerance:
    sys.stdout.write("k-means converged at centroids: [")
    sys.stdout.write(",".join(str(v) for v in centroids))
    sys.stdout.write("]\n")
    converged = True
    break
  last_centroids = centroids[:]
  initial_centroids = ""
  for i in range(k):
    initial_centroids = initial_centroids + str(last_centroids[i])
    if i!=k-1:
      initial_centroids = initial_centroids + ":"
  iter_num += 1

if not converged:
  print("not converge after " + str(iter_num) + " iterations")
  sys.stdout.write("last centroids: [")
  sys.stdout.write(",".join(str(v) for v in last_centroids))
  sys.stdout.write("]\n")
```

##

```{.java style="font-size:12px"}
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class FindCentroid extends EvalFunc<Double> {
  double[] centroids;
  public FindCentroid(String initialCentroid) {
    String[] centroidStrings = initialCentroid.split(":");
    centroids = new double[centroidStrings.length];
    for (int i=0;i<centroidStrings.length;i++)
      centroids[i] = Double.parseDouble(centroidStrings[i]);
  }
  @Override
  public Double exec(Tuple input) throws IOException {
    double min_distance = Double.MAX_VALUE;
    double closest_centroid = 0;
    for (double centroid : centroids) {
      double distance = Math.abs(centroid - (Double)input.get(0));
      if (distance < min_distance) {
        min_distance = distance;
        closest_centroid = centroid;
      }
    }
    return closest_centroid;
  }

}
```

# Read and Write

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

# Identity

## 

```r
  mapreduce(
    input, 
    map = function(k, v) keyval(k, v))
```


# Filter

## 
<ul class="incremental" style="list-style: none" >
<li>

```r
  predicate = 
    function(., v) v%%2 == 0
```

<li> 

```r
  mapreduce(
    input, 
    map = 
      function(k, v) {
        filter = predicate(k, v)
        keyval(k[filter], v[filter])}
```

</ul>

# Select

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

# Sum

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

# Group and Aggregate

## 


```r
  input.ga = 
    to.dfs(
      cbind(
        1:input.size,
        rnorm(input.size)))
```


## 


```r
  group = function(x) x%%10
  aggregate = function(x) sum(x)
```


## 


```r
  mapreduce(
    input.ga, 
      map = 
        function(k, v) 
          keyval(group(v[,1]), v[,2]),
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
    mapreduce(
      input = input ,
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

# K-means

##


```r
    dist.fun = 
      function(C, P) {
        apply(
          C,
          1, 
          function(x) 
            colSums((t(P) - x)^2))}
```

 
##

```r
    kmeans.map = 
      function(., P) {
        nearest = {
          if(is.null(C)) 
            sample(
              1:num.clusters, 
              nrow(P), 
              replace = T)
          else {
            D = dist.fun(C, P)
            nearest = max.col(-D)}}
        if(!(combine || in.mem.combine))
          keyval(nearest, P) 
        else 
          keyval(nearest, cbind(1, P))}
```


##

```r
    kmeans.reduce = {
      if (!(combine || in.mem.combine) ) 
        function(., P) 
          t(as.matrix(apply(P, 2, mean)))
      else 
        function(k, P) 
          keyval(
            k, 
            t(as.matrix(apply(P, 2, sum))))}
```


##

```r
kmeans.mr = 
  function(
    P, 
    num.clusters, 
    num.iter, 
    combine, 
    in.mem.combine) {
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
              map = kmeans.map,
              reduce = kmeans.reduce)))
      if(combine || in.mem.combine)
        C = C[, -1]/C[, 1]
      points(C, col = i + 1, pch = 19)
```

##

```r
      if(nrow(C) < num.clusters) {
        C = 
          rbind(
            C,
            matrix(
              rnorm(
                (num.clusters - 
                   nrow(C)) * nrow(C)), 
              ncol = nrow(C)) %*% C) }}
        C}
```

##


```r
  P = 
    do.call(
      rbind, 
      rep(
        list(
          matrix(
            rnorm(10, sd = 10), 
            ncol=2)), 
        20)) + 
    matrix(rnorm(200), ncol =2)
  x11()
  plot(P)
  points(P)
```


##


```r
    kmeans.mr(
      to.dfs(P),
      num.clusters = 12, 
      num.iter = 5,
      combine = FALSE,
      in.mem.combine = FALSE)
```


<details>
Other features: easy composition of jobs, joins, local modes, combine 
</details>

##

#### Revolution Analytics
### rhadoop@revolutionanalytics.com
#### Antonio Piccolboni
### antonio@piccolboni.info
