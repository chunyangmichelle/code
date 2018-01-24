import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

users = sc.textFile(sys.argv[1])
user=users.map(lambda x: x.split("::")).map(lambda y: (y[0],(y[1])))
ratings = sc.textFile(sys.argv[2])
rating=ratings.map(lambda x: x.split("::")).map(lambda y: (y[0],(int(y[1]),float(y[2]))))

join=rating.join(user).map(lambda (k,((v1,v2),v3)):(v1,v2,v3))

movies = sc.textFile(sys.argv[3])
movie=movies.map(lambda x: x.split("::")).map(lambda y: (int(y[0]),(y[2])))

rating2=join.map(lambda(v1,v2,v3):(v1,(v3,v2)))

join2=movie.join(rating2).map(lambda (k,(v1,(v2,v3))):(v1,v2,v3))

join3=join2.map(lambda(v1,v2,v3):((v1,v2),v3)).mapValues(lambda v: (v,1))
result2=join3.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).mapValues(lambda v: v[0]/v[1]).sortByKey()

task2=result2.map(lambda (x,y): (x[0],x[1],y)).map(lambda (x,y,z): x +","+ y + ","+ str(z))

task2.coalesce(1).saveAsTextFile("Chun_Yang_result_task2.txt")





