from pyspark import SparkContext

sc = SparkContext('local[*]', 'ex1_word_count')

text = """This is example of spark application in Python
Python is very common development language and it also one of Spark supported
languages
The library of Spark in Python called PySpark
In this example you will implements word count application using PySpark
Good luck!!""".split("\n")

#print(text)

rdd = sc.parallelize(text)
#print(rdd.take(10))

w = rdd.flatMap(lambda x: x.split())
#print(w.take(10))

kv = w.map(lambda x: (x, 1))
#print(kv.take(10))

rk = kv.reduceByKey(lambda x, y: x+y)

for i in rk.collect():
    print(f">>>This is how many times the word \"{i[0]}\" appears in the text: {i[1]}")

sc.stop()