hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.insert("testtable",list( list(1,c("x","y","z"),list(1,1,1)))))
checkTrue(hb.insert("testtable",list( list(2,c("x","y","z"),list(1,1,1)),
                                      list(3,c("x:a","y:b","z:b"),list(1,1,1)),
                                      list(4,c("x:c","y:c"),list(1,1)),
                                      list(5,c("x:d"),list(1)))))
checkTrue({
  x <- hb.get("testtable",1)
  rs <- c()

  rs <- c(rs, x[[1]][[1]]==1, all(x[[1]][[2]]==c("x:","y:","z:")),all(unlist(x[[1]][[3]])==1))

  x <- hb.get("testtable",3,"x")
  rs <- c(rs, x[[1]][[1]]==3, all(x[[1]][[2]]==c("x:a")),all(unlist(x[[1]][[3]])==1))

  x <- hb.get("testtable",list(4,5),"x")

  rs <- c(rs,all(unlist(lapply(x,"[[",1))==c(4,5)), all(unlist(lapply(x,"[[",2))==c("x:c","x:d")))
  
  all(rs)
})
                                     
                                     
                                     
