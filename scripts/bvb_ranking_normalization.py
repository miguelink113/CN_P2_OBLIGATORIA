import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, sum as spark_sum, avg
from awsglue.dynamicframe import DynamicFrame

def main():
    # 1. Obtener parámetros del Job de Glue
    # Estos coinciden con los --default-arguments del script de PowerShell
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # 2. Cargar datos desde el Catálogo de Glue (Data Catalog)
    # Importante: El Crawler debe haber terminado para que la tabla exista
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['database'],
        table_name=args['table']
    )
    
    # Convertir a Spark DataFrame para manipulación avanzada
    df = dynamic_frame.toDF()

    # Seleccionar y normalizar columnas
    ranking_df = df.select(
        col("id_player"),
        col("full_name"),
        col("ranking_points"),
        col("team"),
        col("ingestion_timestamp")
    ).orderBy(col("ranking_points").desc())

    # 4. Convertir de nuevo a DynamicFrame para la escritura optimizada de Glue
    output_dynamic_frame = DynamicFrame.fromDF(ranking_df, glueContext, "output_dynamic_frame")
    
    # 5. Guardar resultados en S3 en formato Parquet
    # Parquet es el estándar en Big Data para datos procesados
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": args['output_path']
        },
        format="parquet",
        format_options={
            "compression": "snappy" # Compresión eficiente para ahorrar costes S3
        }
    )

if __name__ == "__main__":
    main()