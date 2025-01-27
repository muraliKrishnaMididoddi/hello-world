Microsoft Windows [Version 10.0.19045.5371]
(c) Microsoft Corporation. All rights reserved.

C:\Users\ADMIN>python
Python 3.12.4 (tags/v3.12.4:8e8a4ba, Jun  6 2024, 19:30:16) [MSC v.1940 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
>>> data = spark.read.csv("ratings.csv", header=True, inferSchema=True)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'spark' is not defined
>>> # Importing the necessary libraries
>>> from pyspark.sql import SparkSession
>>>
>>> # Initialize a Spark session
>>> spark = SparkSession.builder \
...     .appName("RecommendationEngine") \
...     .getOrCreate()
25/01/27 13:07:25 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: Hadoop home directory winutils is not an absolute path. -see https://wiki.apache.org/hadoop/WindowsProblems
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/01/27 13:07:26 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/01/27 13:07:28 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
>>>

>>> data = spark.read.csv(r"C:\Users\ADMIN\spark-3.5.4-bin-hadoop3\spark-3.5.4-bin-hadoop3\ml-latest-small\ml-latest-small\ratings.csv", header=True, inferSchema=True)
>>>
>>> data.show(5)
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
+------+-------+------+---------+
only showing top 5 rows

>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder \
...     .appName("RecommendationEngine") \
...     .config("spark.sql.shuffle.partitions", "2") \
...     .getOrCreate()
>>>
>>> data.printSchema()
root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: integer (nullable = true)

>>> data.show(5)
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
+------+-------+------+---------+
only showing top 5 rows

>>> from pyspark.sql import SparkSession
>>>
>>> # Initialize SparkSession
>>> spark = SparkSession.builder.appName("TestSpark").getOrCreate()
25/01/27 13:14:03 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
>>>
>>> # Initialize SparkSession
>>> spark = SparkSession.builder.appName("TestSpark").getOrCreate()
>>>
>>> data = spark.read.csv(r"C:\Users\ADMIN\spark-3.5.4-bin-hadoop3\spark-3.5.4-bin-hadoop3\ml-latest-small\ml-latest-small\ratings.csv", header=True, inferSchema=True)
>>> data.show(5)
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
+------+-------+------+---------+
only showing top 5 rows

>>> ratings = data.select("userId", "movieId", "rating")
>>> ratings = ratings.dropna()
>>> data.show()
+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      1|   4.0|964982703|
|     1|      3|   4.0|964981247|
|     1|      6|   4.0|964982224|
|     1|     47|   5.0|964983815|
|     1|     50|   5.0|964982931|
|     1|     70|   3.0|964982400|
|     1|    101|   5.0|964980868|
|     1|    110|   4.0|964982176|
|     1|    151|   5.0|964984041|
|     1|    157|   5.0|964984100|
|     1|    163|   5.0|964983650|
|     1|    216|   5.0|964981208|
|     1|    223|   3.0|964980985|
|     1|    231|   5.0|964981179|
|     1|    235|   4.0|964980908|
|     1|    260|   5.0|964981680|
|     1|    296|   3.0|964982967|
|     1|    316|   3.0|964982310|
|     1|    333|   5.0|964981179|
|     1|    349|   4.0|964982563|
+------+-------+------+---------+
only showing top 20 rows

>>> # Split data into training and testing
>>> train_data, test_data = ratings.randomSplit([0.8, 0.2])
>>> from pyspark.ml.recommendation import ALS
>>>
>>> # Initialize ALS model
>>> als = ALS(
...     maxIter=10,
...     regParam=0.1,
...     userCol="userId",
...     itemCol="movieId",
...     ratingCol="rating",
...     coldStartStrategy="drop"  # To handle missing predictions
... )
>>>
>>> # Train the model
>>> als_model = als.fit(train_data)
25/01/27 13:23:25 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
25/01/27 13:23:26 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.lapack.JNILAPACK
>>> # Train the model
>>> als_model = als.fit(train_data)
>>> # Predict ratings for the test set
>>> predictions = als_model.transform(test_data)
>>>
>>> # Display predictions
>>> predictions.show()
+------+-------+------+----------+
|userId|movieId|rating|prediction|
+------+-------+------+----------+
|     2|   6874|   4.0|  3.549033|
|     2|  48516|   4.0|   3.89648|
|     2|  58559|   4.5| 3.7328992|
|     2|  68157|   4.5| 3.8192983|
|     2|  80489|   4.5| 3.4281318|
|     2|  89774|   5.0|   3.34904|
|     2| 106782|   5.0|  3.847223|
|     2| 114060|   2.0| 2.7648308|
|     2| 122882|   5.0| 3.2953243|
|     4|     32|   2.0| 3.1269696|
|     4|    126|   1.0| 1.9432075|
|     4|    171|   3.0|    2.1534|
|     4|    260|   5.0| 3.9734385|
|     4|    296|   1.0| 3.3151767|
|     4|    319|   5.0| 2.4684308|
|     4|    441|   1.0|  4.401853|
|     4|    553|   2.0|  3.365322|
|     4|    759|   3.0| 2.8137505|
|     4|    800|   4.0| 3.7113686|
|     4|    898|   5.0| 4.0114956|
+------+-------+------+----------+
only showing top 20 rows

>>> from pyspark.ml.evaluation import RegressionEvaluator
>>>
>>> # Initialize evaluator
>>> evaluator = RegressionEvaluator(
...     metricName="rmse",
...     labelCol="rating",
...     predictionCol="prediction"
... )
>>>
>>> # Compute RMSE
>>> rmse = evaluator.evaluate(predictions)
>>> print(f"Root-Mean-Square Error: {rmse}")
Root-Mean-Square Error: 0.8722966401721832
>>> # Recommend top 10 movies for each user
>>> user_recs = als_model.recommendForAllUsers(10)
>>> user_recs.show()
+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|     1|[{33649, 5.735495...|
|     2|[{51931, 4.715369...|
|     3|[{74754, 5.320572...|
|     4|[{7700, 5.3952847...|
|     5|[{1237, 4.7694764...|
|     6|[{4429, 4.983449}...|
|     7|[{4441, 5.1142254...|
|     8|[{3379, 5.062689}...|
|     9|[{177593, 5.38124...|
|    10|[{3682, 4.779654}...|
|    11|[{5867, 4.9652333...|
|    12|[{5867, 5.913162}...|
|    13|[{4441, 5.1979227...|
|    14|[{2693, 5.138702}...|
|    15|[{27611, 5.556391...|
|    16|[{3379, 4.584822}...|
|    17|[{33649, 4.949256...|
|    18|[{3379, 4.9277406...|
|    19|[{177593, 4.01239...|
|    20|[{177593, 5.44975...|
+------+--------------------+
only showing top 20 rows

>>> # Recommend top 10 users for each movie
>>> item_recs = als_model.recommendForAllItems(10)
>>> item_recs.show()
+-------+--------------------+
|movieId|     recommendations|
+-------+--------------------+
|      2|[{53, 4.925888}, ...|
|      4|[{543, 3.3080232}...|
|      5|[{43, 4.4900355},...|
|     10|[{543, 4.920768},...|
|     12|[{98, 4.359644}, ...|
|     13|[{43, 4.1300297},...|
|     14|[{53, 5.2730417},...|
|     18|[{53, 5.3985605},...|
|     22|[{53, 4.689987}, ...|
|     25|[{375, 4.821991},...|
|     28|[{43, 5.765923}, ...|
|     31|[{12, 4.593463}, ...|
|     32|[{53, 5.675885}, ...|
|     36|[{568, 4.9168787}...|
|     38|[{43, 3.7045205},...|
|     45|[{53, 4.592192}, ...|
|     46|[{12, 4.5351844},...|
|     49|[{53, 3.4712117},...|
|     50|[{53, 5.7776484},...|
|     52|[{587, 4.382779},...|
+-------+--------------------+
only showing top 20 rows

>>> # Save the model
