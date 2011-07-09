hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.insert("testtable",list(list(100,c("x:","x:1"),list(1,"a")))))
checkTrue({
  rs <- c()
  x <- hb.get("testtable",100,"x:")
  rc <- c(rs, all(x[[1]][[2]]==c("x:","x:1")), all( unlist(x[[1]][[3]])==c("1","a")))

  x <- hb.get("testtable",100,"x:a")
  rc <- c(rc,is.null(x[[1]][[2]]),length(x[[1]][[3]])==0)
  all(rc)
})
