from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import NGram
from pathlib import Path
import re
'''
1- Pegar nomes de airbnbs mais comuns - unigrama e bigrama
2- Distinct host locations
3- host locations que possuem host_since mais antigos
4- Clusterizar latitude e longitude
5- Entender a razão entre aceitação e preço
6- Tentar clusterizar o property type para saber quais tem o "mesmo significado" e entender a relação com a localização
7- Entender o perfil do cliente que não aceita a taxa de aceitação
'''

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
            a = re.sub(r'[^a-zA-Z]', '', a)
            b = re.sub(r'[^a-zA-Z]', '', b)
            bigram = f'{a} {b}'.strip()
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
