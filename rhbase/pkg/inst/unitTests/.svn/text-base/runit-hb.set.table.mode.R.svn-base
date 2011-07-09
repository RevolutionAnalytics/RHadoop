## run after runit-hb.new.table.R
hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.set.table.mode("testttable"))
checkTrue(hb.set.table.mode("testtable",enable=TRUE))
checkTrue(!hb.set.table.mode("testtable",enable=FALSE))
checkTrue(hb.set.table.mode("testtable",enable=TRUE))
