from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from kneed import KneeLocator as ElbowLocator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, udf
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType
from pathlib import Path
from math import ceil, floor

'''
Clusterizar latitude e longitude
'''
spark = SparkSession.builder.master("local[1]").appName('LatLngClusterization').getOrCreate()

# input_path = (Path(__file__).parent.parent / 'input' / 'London_Airbnb_Listings_March_2023.csv')
# input_path = (Path(__file__).parent.parent / 'input' / 'asdf.csv')
# df = spark.read.csv(str(input_path), header=True, inferSchema=True)
input_path = (Path(__file__).parent.parent / 'input' / 'asdf.csv')
df = spark.read.csv('hdfs://localhost:9000//input/asdf.csv', header=True, inferSchema=True)
max_qty_of_clusters = 30
seed = 1

points = df.select(['latitude', 'longitude']) \
    .withColumn("latitude",col("latitude").cast(DoubleType())) \
    .withColumn("longitude",col("longitude").cast(DoubleType())) \


vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
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
    return prediction.withColumn('lat', split1_udf('features')).withColumn('lng', split2_udf('features'))

points = points.select('features')
sum_of_squared_errors = create_variance_curve(points)
plot_variance_curve(sum_of_squared_errors)
k_value = finding_best_k_value(sum_of_squared_errors)
create_clusters(k_value, points)# 11

spark.stop()