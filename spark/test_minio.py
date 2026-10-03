from pyspark.sql import SparkSession


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-MinIO-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# CONFIGURATION MINIO / S3A
# ============================================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.endpoint",
    "http://minio:9000"
)

hadoop_conf.set(
    "fs.s3a.access.key",
    "admin"
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    "industrypulse123"
)

hadoop_conf.set(
    "fs.s3a.path.style.access",
    "true"
)

hadoop_conf.set(
    "fs.s3a.connection.ssl.enabled",
    "false"
)


# ============================================================
# TEST DATA
# ============================================================

data = [
    ("M001", 70.5, "RUNNING"),
    ("M002", 85.2, "WARNING"),
    ("M003", 65.8, "RUNNING")
]

columns = [
    "machine_id",
    "temperature",
    "status"
]

df = spark.createDataFrame(
    data,
    columns
)


print("\n========================================")
print(" TEST SPARK -> MINIO")
print("========================================")

df.show()


# ============================================================
# WRITE TO MINIO
# ============================================================

path = "s3a://industrypulse/test/spark-minio"

print("\nEcriture dans MinIO...")

(
    df.write
    .mode("overwrite")
    .parquet(path)
)

print("Ecriture réussie !")


# ============================================================
# READ FROM MINIO
# ============================================================

print("\nLecture depuis MinIO...")

df_read = spark.read.parquet(path)

df_read.show()

print(
    f"Nombre de lignes relues : {df_read.count()}"
)

print("\nSPARK <-> MINIO : OK")

spark.stop()