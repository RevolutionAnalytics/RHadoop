hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.insert("testtable",list(list(1001,c("x"),list(1)))))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  x[[1]][[3]]==1
})
checkTrue(hb.insert("testtable",list(list(1001,c("x"),list(2)))))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  x[[1]][[3]]==2
})

checkTrue(hb.delete("testtable",1001))
checkTrue({
  x <- hb.get("testtable",1001,"x")
  is.null(x[[1]][[2]]) && length(x[[1]][[3]])==0
})

