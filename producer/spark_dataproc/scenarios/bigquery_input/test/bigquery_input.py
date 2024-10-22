#!/usr/bin/env python

import sys
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Reading from BigQuery').getOrCreate()

table = 'bigquery-public-data.samples.shakespeare'
df = spark.read.format('bigquery').load(table)
df = df.select('word', 'word_count')

df = df.where("word_count > 0 AND word='spark'")
df = df.groupBy('word').sum('word_count')

df.coalesce(1).write.csv(f"file:///tmp/outputs/{round(time.time() * 1000)}")
