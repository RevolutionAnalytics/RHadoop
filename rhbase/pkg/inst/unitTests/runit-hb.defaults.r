hb.init(host='127.0.0.1',port=9090)
y <- hb.defaults()
checkTrue(all(names(y) %in% c("opt.names","sz","usz")))
checkTrue(is.null(hb.defaults("nothere")))

