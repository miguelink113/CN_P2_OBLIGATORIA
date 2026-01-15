import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, when
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

# 3. Transform – Clasificación por puntos
df = df.withColumn("Puntos", col("Puntos").cast(DoubleType()))

df_categoria = df.withColumn(
    "Categoria",
    when(col("Puntos") >= 10000, "Platino")
    .when(col("Puntos") >= 1000, "Oro")
    .when(col("Puntos") >= 100, "Plata")
    .otherwise("Bronce")
)

df_categoria = df_categoria.repartition("Categoria")

# 4. Write to S3 (Parquet)
output_dyf = DynamicFrame.fromDF(
    df_categoria,
    glueContext,
    "output_dyf"
)

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["Categoria"]
    },
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="datasink"
)

job.commit()
