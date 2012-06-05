





##Caveats

Because of the inefficiency of the native R serialization on small objects, even using a C implementation, we decided to
switch automatically to a different serialization format,
 typedbytes, when the vectorized API is used. This means that we are not going to enjoy the same kind of absolute compatibility with R
 but this usually is not a huge drawback when dealing with small records. For example, if each record contains a 100x100 
 submatrix of a larger matrix, the vectorized API doesn't support matrix directly but it's also not going to give a big speed 
 boost, if any. If one is processing graphs and each record is a just an integer pair `(start, stop)`, that's where the
 vectorized interface gives the biggest speed boost and typedbytes serialization is adequate for simple records.
 


```r
from.dfs(input, vectorized = input.size)
```



