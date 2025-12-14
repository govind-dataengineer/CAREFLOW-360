from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql import Window

silver_path = "abfss://silver@stcareflow360.dfs.core.windows.net/patient_flow"
gold_dim_patient = "abfss://gold@stcareflow360.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://gold@stcareflow360.dfs.core.windows.net/dim_department"
gold_fact = "abfss://gold@stcareflow360.dfs.core.windows.net/fact_patient_flow"

w = Window.partitionBy('patient_id').orderBy(F.col('admission_time').desc())
silver_data = spark.read.format('delta').load(silver_path) \
                    .withColumn("row_num",F.row_number().over(w)) \
                    .filter(F.col('row_num') == 1) \
                    .drop('row_num')


incoming_patient = silver_data.select("patient_id",'gender','age') \
                                .withColumn('effective_date',F.current_timestamp()) \
                                .withColumn('_has',F.sha2(
                                                F.concat_ws("||",
                                                    F.coalesce(F.col('gender'),F.lit('NA')),
                                                    F.coalesce(F.col('age').cast('string'), F.lit('NA'))
                                                ),256
                                ))


if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    incoming_patient.withColumn('surrogate_key',F.monotonically_increasing_id()) \
                    .withColumn('effective_to', F.lit(None).cast('TIMESTAMP')) \
                    .withColumn('is_current',F.lit(True)) \
                    .write.format('delta') \
                    .mode('overwrite') \
                    .save(gold_dim_patient)   


incoming_patient.createOrReplaceTempView('incoming_patient')


merge_sql = f"""
        merge into delta.`{gold_dim_patient}` as t
        using incoming_patient as i
        on t.patient_id = i.patient_id and t.is_current = true

        when matched and t._has <> i._has then 
                update set
                        t.is_current = false, 
                        t.effective_to = current_timestamp()
        
        when not matched then 
                insert(
                        surrogate_key,
                        patient_id,
                        gender,
                        age,
                        effective_date,
                        effective_to,
                        is_current,
                        _has
                        )
                values(
                        monotonically_increasing_id(),
                        i.patient_id, 
                        i.gender,
                        i.age,
                        i.effective_date,
                        cast(null as timestamp),
                        true,
                        i._has
                        )

"""
spark.sql(merge_sql)



# -------------------------------
# Step 3: Department Dimension
# -------------------------------
incoming_dept = silver_data.select("department", "hospital_id") \
                         .dropDuplicates(["department", "hospital_id"]) \
                         .withColumn("surrogate_key", monotonically_increasing_id())


# display(incoming_dept)
incoming_dept.write.format("delta").mode("overwrite").save(gold_dim_department)



dim_patient_df = spark.read.format("delta").load(gold_dim_patient) \
    .filter(col("is_current") == True) \
    .select(col("surrogate_key").alias("patient_sk"), "patient_id", "gender", "age")

dim_dept_df = spark.read.format("delta").load(gold_dim_department) \
    .select(col("surrogate_key").alias("department_sk"), "department", "hospital_id")

fact_base = silver_data.select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id") \
    .withColumn("admission_date", F.to_date("admission_time"))

fact_enriched = fact_base.join(dim_patient_df, on="patient_id", how="left") \
    .join(dim_dept_df, on=["department", "hospital_id"], how="left") \
    .withColumn("length_of_stay_hours",
                F.round((F.unix_timestamp("discharge_time") - F.unix_timestamp("admission_time")) / 3600.0)) \
    .withColumn("is_currently_admitted",
                F.when(col("discharge_time") > current_timestamp(), lit(True)).otherwise(lit(False))) \
    .withColumn("event_ingestion_time", current_timestamp())

fact_final = fact_enriched.select(
    monotonically_increasing_id().alias("fact_id"),
    "patient_sk", "department_sk",
    "admission_time", "discharge_time", "admission_date",
    "length_of_stay_hours", "is_currently_admitted", "bed_id", "event_ingestion_time"
)

fact_final.write.format("delta").mode("overwrite").partitionBy("admission_date").save(gold_fact)
