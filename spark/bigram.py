from pyspark.sql import SparkSession, Row
from pathlib import Path
import re

spark = SparkSession.builder.master("local[1]").appName('Ngrams').getOrCreate()
input_path = (Path(__file__).parent.parent / 'input' / 'London_Airbnb_Listings_March_2023.csv')
df = spark.read.csv(str(input_path), header=True, inferSchema=True)

def word_pairs(line):
    '''
    Gera bigramas por linha do csv
    '''
    if line is not None:
        words = line.split()
        result = []
        for a, b in zip(words, words[1:]):
            a = re.sub(r'[^a-zA-Z0-9]', '', a)
            b = re.sub(r'[^a-zA-Z0-9]', '', b)
            bigram = (f'{a} {b}'.strip().lower())
            if len(bigram.split()) > 1:
                result.append(bigram)
        return result
    return ''

rdd1 = df.select('name').rdd.map(lambda x: x[0])
pairs = rdd1.flatMap(word_pairs)
count = pairs.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

with open('./result.txt', 'w') as f:
    result = count.collect()
    result.sort(key = lambda x: x[1], reverse=True)
    for i in result:
        f.write(str(i) + '\n')

spark.stop()
