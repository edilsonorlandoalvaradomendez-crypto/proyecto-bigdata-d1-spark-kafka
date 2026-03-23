from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("D1_Batch").getOrCreate()

df = spark.read.csv("data/superstore.csv", header=True, inferSchema=True)

df = df.dropna().dropDuplicates()

print("Datos cargados:")
df.show(5)

ventas_categoria = df.groupBy("Category").sum("Sales")
ventas_categoria.show()

data = ventas_categoria.toPandas()

plt.figure()
plt.bar(data["Category"], data["sum(Sales)"])
plt.title("Ventas por Categoría - D1")
plt.xlabel("Categoría")
plt.ylabel("Ventas")
plt.show()
