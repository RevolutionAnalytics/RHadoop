







# Mapreduce in R

## My first mapreduce job
  
  Conceptually, mapreduce is not very different than a combination of `lapply`s and a `tapply`: transform elements of a list, compute an index &mdash; key in mapreduce jargon &mdash; and process the groups thus defined. Let's start with a simple `lapply` example:
 

```r
  small.ints = 1:1000
  sapply(small.ints, function(x) x^2)
```


The example is trivial, just computing the first 10 squares, but we just want to get the basics here, there are interesting examples later on. Now to the mapreduce equivalent:


```r
  small.ints = to.dfs(1:1000)
  mapreduce(
    input = small.ints, 
    map = function(k, v) cbind(v, v^2))
```



This is all it takes to write your first mapreduce job in `rmr`. There are some differences that we will review, but the first thing to notice is that it isn't all that different, and just two lines of code. The first line puts the data into HDFS, where the bulk of the data has to reside for mapreduce to operate on. It is not possible to write out big data with `to.dfs`, not in a scalable way. `to.dfs` is nonetheless very useful for a variety of uses like writing test cases, learning and debugging. `to.dfs` can put the data in a file of your own choosing, but if you don't specify one it will create temp files and clean them up when done. The return value is something we call a *big data object*. You can assign it to variables, pass it to other `rmr` functions, mapreduce jobs or read it back in. It is a stub, that is the data is not in memory, only some information that helps finding and managing the data. This way you can refer to very large data sets whose size exceeds memory limits. 

Now onto the second line. It has `mapreduce` replace `lapply`. We prefer named arguments with `mapreduce` because there's quite a few possible arguments, but it's not mandatory. The input is the variable `small.ints` which contains the output of `to.dfs`, that is a stub for our small number data set in its HDFS version, but it could be a file path or a list containing a mix of both. The function to apply, which is called a map function as opposed to the reduce function, which we are not using here, is a regular R function with a few constraints:
  
1. It's a function of two arguments, a collection of keys and one of values
1. It returns key value pairs using the function `keyval`, which can have vectors, lists, matrices or data.frames as arguments; you can also return `NULL`. You can avoid calling `keyval` explicitly but the return value `x` will be converted with a call to `keyval(NULL,x)`. This is not allowed in the map function when the reduce function is specified and under no circumstance in the combine function, since specifying the key is necessary for the shuffle phase.

In this example, we are not using the keys at all, only the values, but we still need both to support the general mapreduce case. The return value is big data object, and you can pass it as input to other jobs or read it into memory (watch out, it will fail for big data!) with `from.dfs`. `from.dfs` is complementary to `to.dfs` and returns a key-value pair collection. `from.dfs` is useful in defining map reduce algorithms whenever a mapreduce job produces something of reasonable size, like a summary, that can fit in memory and needs to be inspected to decide on the next steps, or to visualize it. It is much more important than `to.dfs` in production work.

## My second mapreduce job

We've just created a simple job that was logically equivalent to a `lapply` but can run on big data. That job had only a map. Now to the reduce part. The closest equivalent in R is arguably a `tapply`. So here is the example from the R docs:


```r
  groups = rbinom(32, n = 50, prob = 0.4)
  tapply(groups, groups, length)
```

This creates a sample from the binomial and counts how many times each outcome occurred. Now onto the mapreduce  equivalent:
  

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


First we move the data into HDFS with `to.dfs`. As we said earlier, this is not the normal way in which big data will enter HDFS; it is normally the responsibility of scalable data collection systems such as Flume or Sqoop. In that case we would just specify the HDFS path to the data as input to `mapreduce`. But in this case the input is the variable `groups` which contains a big data object, which keeps track of where the data is and does the clean up when the data is no longer needed. Since a map function is not specified it is set to the default, which is like an identity but consistent with the map requirements, that is `function(k,v) keyval(k,v)`. The reduce function takes two arguments, one is a key and the other is a collection of all the values associated with that key. It could be one of vector, list, data frame or matrix depending on what was returned by the map function. The idea is that if the user returned values of one class, we should preserve that through the shuffle phase. Like in the map case, the reduce function can return `NULL`, a key-value pair as generated by the function `keyval` or any other object `x` which is equivalent to `keyval(NULL, x)`. The default is no reduce, that is the output of the map is the output of mapreduce. In this case the keys are realizations of the binomial and the values are all `1` (please note recycling in action) and the only important  thing is how many there are, so `length` gets the job done. Looking back at this second example, there are some small differences with `tapply` but the overall complexity is very similar.

## Word count

The word count program has become a sort of "hello world" of the mapreduce world. For a review of how the same task can be accomplished in several languages, but always for mapreduce, see this [blog entry](http://blog.piccolboni.info/2011/04/looking-for-map-reduce-language.html).

We are defining a function, `wordcount`, that encapsulates this job. This may not look like a big deal but it is important. Our main goal was not simply to make it easy to run a mapreduce job but to make mapreduce jobs first class citizens of the R environment and to make it easy to create abstractions based on them. For instance, we wanted to be able to assign the result of a mapreduce job to a variable &mdash; and I mean *the result*, not some error code or diagnostics &mdash; and to create complex expressions including mapreduce jobs. We take the first step here by creating a function that is itself a job, can be chained with other jobs, executed in a loop etc.  

Let's now look at the signature. 


```r
wordcount = 
  function(
    input, 
    output = NULL, 
    pattern = " "){
```


There is an input and optional output and a pattern that defines what a word is. 


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


The map function, as we know already, takes two arguments, a key and a value. The key here is not important, indeed always `NULL`. The value contains several lines of text, which gets split according to a pattern. Here you can see that `pattern` is accessible in the mapper without any particular work on the programmer side and according to normal R scope rules. This apparent simplicity hides the fact that the map function is executed in a different interpreter and on a different machine than the `mapreduce` function. Behind the scenes the R environment is serialized, broadcast to the cluster and restored on each interpreter running on the nodes. For each word, a key value pair (*w*, 1) is generated with `keyval` and their collection is the return value of the mapper. 


```r
    wc.reduce =
      function(word, counts ) {
        keyval(word, sum(counts))}
```


The reduce function takes a key and a collection of values, in this case a numeric vector, as input and simply sums up all the counts and returns the pair word, count using the same helper function, `keyval`. Finally, specifying the use of a combiner is necessary to guarantee the scalability of this algorithm. Now on to the mapreduce call.


```r
    mapreduce(
      input = input ,
      output = output,
      input.format = "text",
      map = wc.map,
      reduce = wc.reduce,
      combine = T)}
```


The implementation defines map and reduce functions and then makes a single call to `mapreduce`. The map and reduce functions could be as well anonymous functions as they are used only once, but there is one advantage in naming them. `rmr` offers alternate backends, in particular one can switch off Hadoop altogether with `rmr.options(backend = "local")`. While this is of no use for production work, as it offers no scalability, it is an amazing resource for learning and debugging as we are dealing with a local, run of the mill R program with the same semantics as when run on top of Hadoop. This, in combination with using named map and reduce functions, allows us to use `debug` to debug mapper and reducer the familiar way. 

The input can be an HDFS path, the return value of `to.dfs` or another job or a list thereof &mdash; potentially, a mix of all three cases, as in `list("a/long/path", to.dfs(...), mapreduce(...), ...)`. The output can be an HDFS path but if it is `NULL` some temporary file will be generated and wrapped in a big data object, like the ones generated by `to.dfs`. In either event, the job will return the information about the output, either the path or the big data object. Therefore, we simply pass along the input and output of the`wordcount` function to the `mapreduce` call and return whatever its return value. That way the new function also behaves like a proper mapreduce job &mdash; more details [here](Writing-composable-mapreduce-jobs). The `input.format` argument allows us to specify the format of the input. The default is based on R's own serialization functions and supports all R data types. In this case we just want to read a text file, so the `"text"` format will create key value pairs with a `NULL`key and a line of text as value. You can easily specify your own input and output formats and even accept and produce binary formats with the functions `make.input.format` and `make.output.format`. 



## Logistic Regression

Now on to an example from supervised learning, specifically logistic regression by gradient descent. Again we are going to create a function that encapsulates this algorithm. 


```r
logistic.regression = 
  function(input, iterations, dims, alpha){
```


As you can see we have an input representing the training data. For simplicity we ask to specify a fixed number of iterations, but it would be only slightly more difficult to implement a convergence criterion. Then we need to specify the dimension of the problem, which is redundant because it can be inferred after seeing the first line of input, but we didn't want to put additional logic in the map function, and then we have the learning rate `alpha`. Now we are going to make a slight departure from the actual order in which the code is written. The source code goes on to define the map and reduce functions, but we are going to delay their presentation slightly.


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


We start by initializing the separating plane and defining the logistic function. As before, those variables will be used inside the map function, that is they will travel across interpreter and processor and network barriers to be available where the developer needs them and where a traditional, meaning sequential, R developer expects them to be available according to scope rules &mdash; no boilerplate code and familiar, powerful behavior.

Then we have the main loop where computing the gradient of the loss function is the duty of a map reduce job, whose output is brought into main memory with a call to `from.dfs` &mdash; any intermediate result files are managed by the system, not you. The only important thing you need to know is that the gradient is going to fit in memory so we can call `from.dfs` to get it without exceeding available  resources.


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


The map function simply computes the contribution of a subset of points to the gradient. Please note the variables `g` and `plane` making their necessary appearance here without any work on the developer's part. The access here is read only but you could even modify them if you wanted &mdash; the semantics is copy on assign, which is consistent with how R works and easily supported by Hadoop. Since in the next step we just want to add everything together, we return a dummy, constant key for each value. Note the use of recycling in `keyval`.


```r
  lr.reduce =
    function(k, Z) 
      keyval(k, t(as.matrix(apply(Z,2,sum))))
```


The reduce function is just a big sum. Since we have only one key, all the work will fall on one reducer and that's not scalable. Therefore, in this example, it's important to activate the combiner, in this case set to TRUE, which means same as the reducer. Since sums are associative and commutative that's all we need. `mapreduce` also accepts a distinct combiner function. Remember that a combiner's arguments can come from a map or a combine function and its return value can go to a combine or reduce function. Finally, a reminder that both map and reduce functions need to be defined inside the logistic regression function to have access to the `g` function and the `plane` vector, so cutting and pasting this code in this order won't work.

To make this example production-level there are several things one needs to do, like having a convergence criterion instead of a fixed iterations number an an adaptive learning rate,  but probably gradient descent just requires too many iterations to be the right approach in a big data context. But this example should give you all the elements to be able to implement, say, conjugate gradient instead. In general, when each iteration requires I/O of a large data set, the number of iterations needs to be modest and algorithms with O(log(N)) number of iterations are natural candidates, even if the work in each iteration may be more substantial.

## K-means

We are now going to cover a simple but significant clustering algorithm and the complexity will go up just a little bit. To cheer yourself up, you can take a look at [this alternative implementation](http://www.hortonworks.com/new-apache-pig-features-part-2-embedding/) which requires three languages, python, pig and java, to get the job done and is hailed as a model of simplicity.

We are talking about k-means. This is not a production ready implementation, but should be illustrative of the power of this package. You simply can not do this in pig or hive alone and it would take hundreds of lines of code in java.


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


This is simply a distance function, the only noteworthy property of which is that it can compute all the distance between a matrix of centers `C` and a matrix of points `P` very efficiently, on my laptop it can do 10^6 points and 10^2 centers in 5 dimensions in approx. 16s. The only explicit iteration is over the dimension, but all the other operations are vectorized (e.g. loops are pushed to the C library), hence the speed.


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

The role of the map function is to compute distances between some points and all centers and return for each point the closest center. It has two flavors controlled by the main `if`: the first iteration when no candidate centers are available and all the following ones. Please note that while the points are stored in HDFS and provided to the map function as its second argument, the centers are simply stored in a matrix and available in the map function because of normal scope rules. In the first iteration, each point is randomly assigned to a center, whereas in the following ones a min distance criterion is used. Finally notice the vectorized use of keyval whereby all the center-point pairs are returned in one statement (the correspondence is positional, with the second dimension used when present).


```r
    kmeans.reduce.1 = 
      function(x, P) {
        t(as.matrix(apply(P, 2, mean)))}
```


The reduce function couldn't be simpler as it just computes column averages of a matrix of points sharing a center, which is the key. If you  wonder why the `.1` in the name of these two functions, it's because we plan to explore some optimizations in future installments.


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

The main loop does nothing but bring into memory the result of a mapreduce job with the two above functions as mapper and reducer and the big data object with the points as input. Once the keys are discarded, the values form a matrix which become the new centers. The last two lines before the return value are a heuristic to keep the number of centers the desired one (when centers are nearest to no points, they are lost). To run this function we need some data:


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


  That is, create a large matrix with a few rows  repeated many times and then add some noise. Finally, the function call:
  

```r
    kmeans.mr(
      to.dfs(input),
      num.clusters  = 12, 
      num.iter= 5)
```


With a little extra work you can even get pretty visualizations like [this one](kmeans.gif).

## Linear Least Squares
  
This is an example of a hybrid mapreduce-conventional solution to a well known problem. We will start with a mapreduce job that results in a smaller data set that can be brought into main memory and processed in a single R instance. This is straightforward in rmr because of the simple primitive that transfers data into memory, `from.dfs`, and the R-native data model. This is in contrast with hybrid pig-java-python solutions where mapping data types from one language to the other is a time-consuming and error-prone chore the developer has to deal with.

Specifically, we want to solve LLS under the assumption that we have too many data points to fit in memory but not such a huge number of variables that we need to implement the whole process as map reduce job. This is the basic equation we want to solve in the least square sense: 
  
  **X** **&beta;** = **y**
  
  We are going to do it by using the function solve as in the following expression, that is solving the normal equations.

<pre>
  solve(t(X)%*%X, t(X)%*%y)
</pre>
  
  But let's assume that X is too big to fit in memory, so we have to compute the transpose and matrix products using map reduce, then we can do the solve as usual on the results. This is our general plan.

Let's get some data first, potentially big data matrix `X` and a regular vector `y`:


```r
X = to.dfs(matrix(rnorm(2000), ncol = 10))
y = as.matrix(rnorm(200))
```


The next is a reusable reduce function that just sums a list of matrices, ignores the key.


```r
Sum = 
  function(., YY) 
    keyval(1, list(Reduce('+', YY)))
```


The big matrix is passed to the mapper in chunks of complete rows. Smaller cross-products are computed for these submatrices and passed on to a single reducer, which sums them together. Since we have a single key a combiner is mandatory and since matrix sum is associative and commutative we certainly can use it here.


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


The same pretty much goes on also for vector y, which is made available to the nodes according to normal scope rules.


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


And finally we just need to call `solve`.


```r
solve(XtX, Xty)
```

   
### Related Links
  
  * [Comparison of high level languages for mapreduce: k means](https://github.com/RevolutionAnalytics/RHadoop/wiki/Comparison-of-high-level-languages-for-mapreduce%3A-k-means)
  * [Changelog](https://github.com/RevolutionAnalytics/RHadoop/wiki/Changelog)
  * [Design philosophy](https://github.com/RevolutionAnalytics/RHadoop/wiki/Design-philosophy)
  * [Efficient rmr techniques](https://github.com/RevolutionAnalytics/RHadoop/wiki/Efficient-rmr-techniques)
  * [Writing composable mapreduce jobs](https://github.com/RevolutionAnalytics/RHadoop/wiki/Writing-composable-mapreduce-jobs)
  * [Use cases](https://github.com/RevolutionAnalytics/RHadoop/wiki/Use-cases)
  * [Getting data in and out](https://github.com/RevolutionAnalytics/RHadoop/wiki/Getting-data-in-and-out)
  * [FAQ](https://github.com/RevolutionAnalytics/RHadoop/wiki/FAQ)

   
