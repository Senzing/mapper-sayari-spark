from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
  "Etl parquet aggregation to json").getOrCreate()
# with s3a:// prefix
dfEntities = spark.read.parquet("s3a path")
dfRelations = spark.read.parquet("s3a path")
joinedDataFrame = dfEntities.join(dfRelations, dfEntities.entity_id == dfRelations.src, "left")
joinedDataFrame = joinedDataFrame.rdd.map(lambda x: (x.entity_id, x)).groupByKey().mapValues(list).\
  reduceByKey(lambda x,y: reduceByEntityIdToJson)
joinedDataFrame.saveAsTextFile("s3a path")



#will return string with json
def reduceByEntityIdToJson(entityId,aggregations):
  return

