from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import json
from pyspark.ml.classification import RandomForestClassificationModel

def create_spark_session():
    spark = SparkSession.builder \
        .appName("DiabetesPredictionStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .config("spark.hadoop.io.nativeio.NativeIO.POSIX.stat", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_models():
        transform_model = PipelineModel.load('model/transform_pipeline')
        prediction_model = RandomForestClassificationModel.load('model/best_model')
        return transform_model, prediction_model

def create_schema():
    try:
        with open('model/model_metadata.json', 'r') as f:
            metadata = json.load(f)
            feature_cols = metadata['feature_cols']
    except:
        feature_cols = [
            'HighBP', 'HighChol', 'CholCheck', 'BMI', 'Smoker',
            'Stroke', 'HeartDiseaseorAttack', 'PhysActivity', 'Fruits',
            'Veggies', 'HvyAlcoholConsump', 'AnyHealthcare', 'NoDocbcCost',
            'GenHlth', 'MentHlth', 'PhysHlth', 'DiffWalk', 'Sex',
            'Age', 'Education', 'Income'
        ]

    schema_fields = [StructField(col, DoubleType(), True) for col in feature_cols]
    schema = StructType(schema_fields)
    return schema, feature_cols


def process_stream(spark, transform_model, prediction_model):
    schema, feature_cols = create_schema()
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'localhost:9092') \
        .option("subscribe", 'health_data') \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    df_transformed = transform_model.transform(df_parsed)
    df_predictions = prediction_model.transform(df_transformed)

    output_cols =  feature_cols + ['prediction', 'probability']
    df_output = df_predictions.select(*[col for col in output_cols if col in df_predictions.columns])

    df_output = df_output.withColumn(
        'probability',
        col('probability').cast('string'))

    df_kafka = df_output.select(
        to_json(struct(*df_output.columns)).alias("value"))

    query = df_kafka \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'localhost:9092') \
        .option("topic", 'health_data_predicted') \
        .option("checkpointLocation", 'checkpoints/streaming') \
        .option("startingOffsets", "latest")\
        .outputMode("append") \
        .start()

    print(f"Streaming started!")
    query_console = df_output \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()

    return query, query_console


def main():
    print("Spark Structured Streaming - Diabetes Prediction")
    spark = create_spark_session()
    transform_model, prediction_model = load_models()
    query, query_console = process_stream(spark, transform_model, prediction_model)
    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    main()
