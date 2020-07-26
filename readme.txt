inceptor(hive)UDAF
如果遇到执行出现:  Invaild signature file digest for Manifest main *** 错误
解决办法:zid -d spark-inceptor.jar META-INF/.RSA META-INF/.DSA  META-INF/.SF
