import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
  "Etl parquet aggregation to json").getOrCreate()
# with s3a:// prefix
resolveable_record_types = []
resolveable_record_types.append('PERSON')
resolveable_record_types.append('COMPANY')
resolveable_record_types.append('AIRCRAFT')
resolveable_record_types.append('VESSEL')
df_entities = spark.read.parquet("s3a path")
df_relations = spark.read.parquet("s3a path")
analysis_mode = False
id_sample_size = 1000000 if analysis_mode else 100
codes_filename = ""
joined_data_frame = df_entities.join(df_relations, df_entities.entity_id == df_relations.src, "left")
joined_data_frame = joined_data_frame.rdd.map(lambda x: (x.entity_id, x)).groupByKey().mapValues(list).\
  reduceByKey(lambda x, y: reduce_by_entity_id_to_json(x, y))
joined_data_frame.saveAsTextFile("s3a path")



#will return string with json
def reduce_by_entity_id_to_json(entityId,aggregations):
  return

