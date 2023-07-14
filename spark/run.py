from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from kneed import KneeLocator as ElbowLocator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, udf, avg
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType, FloatType
from pathlib import Path
from math import ceil, floor
from re import sub

'''
Clusterizar latitude e longitude
'''
spark = SparkSession.builder.master("local[1]").appName('LatLngClusterization').getOrCreate()

output_path='hdfs://localhost:9000//output/'

df = spark.read.csv('hdfs://localhost:9000//input/London_Airbnb_Listings_March_2023.csv', header=True, inferSchema=True)
max_qty_of_clusters = 30
seed = 1

def price_to_float(price):
    return float(sub(r'[^\d.]', '', price))

priceUDF = udf(lambda x: price_to_float(x), FloatType()) 

points = df.select(['latitude', 'longitude', 'price']) \
    .withColumn("lat",col("latitude").cast(DoubleType())) \
    .withColumn("lng",col("longitude").cast(DoubleType())) \
    .withColumn("price", priceUDF(col('price')))
    
vecAssembler = VectorAssembler(inputCols=["lat", "lng"], outputCol="features").setHandleInvalid("skip")
points = vecAssembler.transform(points)


def create_variance_curve(features):
    sum_of_squared_errors = []
    
    for i in range(2, max_qty_of_clusters+1):
        kmeans = KMeans().setK(i).setSeed(seed)
        model = kmeans.fit(features)
        sum_of_squared_errors.append(model.summary.trainingCost)
    
    return sum_of_squared_errors

def plot_variance_curve(sum_of_squared_errors):
    plt.plot(range(2, max_qty_of_clusters+1), sum_of_squared_errors, color ='g', linewidth ='3')
    plt.xlabel("Value of K")
    plt.ylabel("Squared Error (Cost)")
    plt.show() 

def finding_best_k_value(sum_of_squared_errors):
    x = list(range(2, max_qty_of_clusters+1))
    y = sum_of_squared_errors
    kn = ElbowLocator(x, y, curve='convex', direction='decreasing')
    clusters_qty = ceil(kn.elbow) if kn.elbow < 4 else floor(kn.elbow)
    print(f'elbow {clusters_qty}')
    return clusters_qty

def create_clusters(number_of_clusters: int, features, plot=False):
    kmeans = KMeans().setK(number_of_clusters).setSeed(seed)
    model = kmeans.fit(features)
    prediction = model.transform(features)
    split1_udf = udf(lambda value: value[0].item(), DoubleType())
    split2_udf = udf(lambda value: value[1].item(), DoubleType())
    return prediction.withColumn('lat', split1_udf('features')).withColumn('lng', split2_udf('features')).drop('features', 'latitude',  'longitude')

def average_price_per_cluster(clusters):
    return clusters.groupBy('prediction').agg(avg('price').alias("avg_price"))

sum_of_squared_errors = create_variance_curve(points)
plot_variance_curve(sum_of_squared_errors)
k_value = finding_best_k_value(sum_of_squared_errors)
clusters = create_clusters(k_value, points)
avg_price = average_price_per_cluster(clusters)

clusters.write.save(output_path + 'result.parquet', format='parquet', mode='overwrite')
avg_price.write.save(output_path + 'avg_price.parquet', format='parquet', mode='overwrite')

#QUERIES HIVE
#create table localization_cluster (prediction int,  lat double, lng double) STORED AS PARQUET LOCATION 'hdfs://localhost:9000//output/result.parquet';
#select * from localization_cluster limit 10;
#create table clusterized_avg_price (prediction int,  avg_price double) STORED AS PARQUET LOCATION 'hdfs://localhost:9000//output/avg_price.parquet';
#select * from clusterized_avg_price limit 10;

spark.stop()