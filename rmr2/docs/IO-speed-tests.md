```r
zz =rmr2:::interleave(1:10^6, 1:10^6)
con = file("/tmp/n-test", "wb")
system.time({rmr2:::typed.bytes.writer(zz, con, TRUE)})
#   user  system elapsed 
#  0.355   0.058   0.468 
close(con)
con = file("/tmp/tb-test", "wb")
system.time({rmr2:::typed.bytes.writer(zz, con, FALSE)})
#   user  system elapsed 
#  0.310   0.023   0.333 
close(con)
system.time({save(zz, file= "/tmp/save-test")})
#user  system elapsed 
#1.535   0.010   1.546 
system.time({rmr2:::make.typed.bytes.input.format()(file("/tmp/n-test", "rb"), 10^6)})
#user  system elapsed 
# 8.398   0.252   8.650 
system.time({rmr2:::make.typed.bytes.input.format()(file("/tmp/tb-test", "rb"), 10^6)})
#user  system elapsed 
# 6.999   0.212   7.206
system.time({load(file="/tmp/save-test")})
#user  system elapsed 
#0.645   0.009   0.659 
```
