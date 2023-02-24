import csv
import json
from pyspark.sql import SparkSession

def load_codes_file(codes_filename):
  code_conversion_data = {}
  unmapped_code_count = 0
  with open(codes_filename, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
      row['RAW_TYPE'] = row['RAW_TYPE'].upper()
      row['RAW_CODE'] = row['RAW_CODE'].upper()
      if row['RAW_TYPE'] not in code_conversion_data:
        code_conversion_data[row['RAW_TYPE']] = {}
      row['COUNT'] = 0
      row['EXAMPLES'] = {}
      code_conversion_data[row['RAW_TYPE']][row['RAW_CODE']] = row
      if row['REVIEWED'].upper() != 'Y':
        unmapped_code_count += 1
  return code_conversion_data, unmapped_code_count,

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
other_sample_size = 100
id_sample_size = 1000000 if analysis_mode else other_sample_size
codes_filename = ""
code_conversion_data, unmapped_code_count = load_codes_file(codes_filename)
joined_data_frame = df_entities.join(df_relations,
                                     df_entities.entity_id == df_relations.src,
                                     "left")
joined_data_frame.withColumn("code_conversion_data", json.dump(code_conversion_data))
joined_data_frame = joined_data_frame.rdd.map(
  lambda x: (x.entity_id, x)).groupByKey().mapValues(list). \
  reduceByKey(lambda x, y: reduce_by_entity_id_to_json(x, y))
joined_data_frame.saveAsTextFile("s3a path")





# will return string with json
def reduce_by_entity_id_to_json(entityId, aggregations):
  return
