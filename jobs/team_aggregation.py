import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.sql.functions import col, sum as spark_sum, avg, count, round
from pyspark.sql.types import DoubleType

# 1. Init
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'database', 'table_name', 'output_path']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database = args['database']
table_name = args['table_name']
output_path = args['output_path']

# 2. Read from Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=table_name,
    transformation_ctx="datasource"
)

df = datasource.toDF()

# 3. Transform – Aggregate by Team
df = df.withColumn("Puntos", col("Puntos").cast(DoubleType()))

df_equipo = df.groupBy("EquipoVoleyPlaya").agg(
    spark_sum("Puntos").alias("total_puntos_equipo"),
    avg("Puntos").alias("media_puntos_equipo"),
    count("*").alias("numero_jugadores")
)

df_equipo = df_equipo.withColumn(
    "media_puntos_equipo",
    round(col("media_puntos_equipo"), 2)
)

df_equipo = df_equipo.repartition("EquipoVoleyPlaya")

# 4. Write to S3 (Parquet)
output_dyf = DynamicFrame.fromDF(
    df_equipo,
    glueContext,
    "output_dyf"
)

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["EquipoVoleyPlaya"]
    },
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="datasink"
)

job.commit()
