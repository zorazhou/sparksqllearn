from pyspark.sql import SparkSession


def starting1():
    spark = SparkSession.builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.json("C:\\workspace\\sparksqllearn\\resources\\people.txt")
    # df.show()

    # basic df基本操作
    # df.printSchema()
    # df.select("name").show()
    # df.select(df['name'], df['age'] + 1).show()
    # df.filter(df['age'] > 21).show()
    # df.groupBy("age").count().show()

    # Programmatically 编程方式sql查询
    df.createOrReplaceTempView("people")
    sqlDF = spark.sql("SELECT * FROM people")
    # sqlDF.show()

    # Global Temporary View 全局临时视图
    df.createGlobalTempView("people")
    # spark.sql("SELECT * FROM global_temp.people").show()
    # spark.newSession().sql("SELECT * FROM global_temp.people").show()

    # Reflection rdd 反射创建rdd
    from pyspark.sql import Row
    sc = spark.sparkContext
    lines = sc.textFile("C:\\workspace\\sparksqllearn\\resources\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")
    teenagers = spark.sql(
        "SELECT name FROM people WHERE age >= 13 AND age <= 19")
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    # for name in teenNames:
    #     print(name)




starting1()
