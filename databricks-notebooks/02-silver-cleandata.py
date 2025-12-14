from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_path = "abfss://bronze@stcareflow360.dfs.core.windows.net/patient_flow"
silver_path = "abfss://silver@stcareflow360.dfs.core.windows.net/patient_flow"


bronze_df = spark.readStream.format("delta").load(bronze_path)

#define schema

schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

parsed_df = bronze_df.withColumn('parsed', from_json('raw_json', schema)).select('parsed.*') \
                    .withColumn('admission_time', to_timestamp('admission_time')) \
                    .withColumn('discharge_time', to_timestamp('discharge_time')) \

#invalid admission times

clean_df = parsed_df.withColumn("admission_time", 
                                                    when(
                                                        (col('admission_time').isNotNull()) | (col('admission_time') > current_timestamp()), 
                                                        current_timestamp())
                                                        .otherwise(col('admission_time'))
                                ) \
                    .withColumn("age",
                                        when(col('age') > 100,floor(rand()*80 + 1).cast('int'))
                                        .otherwise(col('age'))                                
                                )

(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema","true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)


