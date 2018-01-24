import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

users = sc.textFile(sys.argv[1])
user=users.map(lambda x: x.split("::")).map(lambda y: (y[0],(y[1])))

ratings = sc.textFile(sys.argv[2])
rating=ratings.map(lambda x: x.split("::")).map(lambda y: (y[0],(int(y[1]),float(y[2]))))

join=rating.join(user).map(lambda (k,((v1,v2),v3)):(v1,v2,v3))
join1=join.map(lambda(v1,v2,v3):((v1,v3),v2)).mapValues(lambda v: (v,1))

result=join1.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).mapValues(lambda v: v[0]/v[1]).sortByKey()

task1=result.map(lambda (x,y):(x[0],x[1],y)).map(lambda (x,y,z): str(x) + "," + y + "," + str(z))
task1.coalesce(1).saveAsTextFile("Chun_Yang_result_task1.txt")





