hb.init(host='127.0.0.1',port=9090)
yy <- paste(sample(letters,5),collapse="")
checkTrue(hb.new.table(yy, "x","y","z",opts=list(y=list(compression='GZ'))))
checkTrue(hb.delete.table(yy))

