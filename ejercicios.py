import os, shutil
from operator import add
from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.master("local[*]").appName("EjerciciosRDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def titulo(t):
    print("\n" + "=" * 60)
    print(t)
    print("=" * 60)

# ---------------- Ejercicio 1: Word Count ----------------
titulo("Ejercicio 1: Word Count Basico")
texto = ["hola mundo", "hola spark", "mundo spark"]
conteo = (sc.parallelize(texto)
            .flatMap(lambda f: f.split(" "))
            .map(lambda p: (p, 1))
            .reduceByKey(add))
print("Conteo de palabras:", sorted(conteo.collect()))

# ---------------- Ejercicio 2: Analisis de ventas ----------------
titulo("Ejercicio 2: Analisis de Ventas")
ventas = sc.parallelize([("Madrid", 100), ("Barcelona", 200), ("Madrid", 50)])

total = ventas.reduceByKey(add)
print("a) Suma por ciudad:", total.collect())

mayores = total.filter(lambda x: x[1] > 100)
print("b) Ciudades con ventas > 100:", mayores.collect())

ordenado = mayores.sortBy(lambda x: x[1], ascending=False)
print("c) Ordenado de mayor a menor:", ordenado.collect())

# ---------------- Ejercicio 3: Filtrado y transformacion ----------------
titulo("Ejercicio 3: Filtrado y Transformacion")
precios_usd = sc.parallelize([10, 25, 40, 5, 100, 15])
precios_eur = precios_usd.filter(lambda x: x >= 20).map(lambda x: x * 0.9)
print("Precios >= 20 USD convertidos a EUR:", precios_eur.collect())

# ---------------- Ejercicio 4: Procesamiento de CSV ----------------
titulo("Ejercicio 4: Procesamiento de CSV")
lineas = sc.parallelize(["Leche,Huevos,Pan", "Manzanas,Peras", "Leche,Chocolate"])
productos = lineas.flatMap(lambda l: l.split(","))
print("Productos aplanados:", productos.collect())
unicos = productos.distinct()
print("Productos unicos:", sorted(unicos.collect()))
print("Cantidad de productos unicos:", unicos.count())

# ---------------- Ejercicio 5: Analisis de logs ----------------
titulo("Ejercicio 5: Analisis de Logs")
logs = sc.textFile("datos/logs.txt")                                   # 1. cargar
peticiones = (logs.map(lambda l: (l.split(",")[0], 1))
                  .reduceByKey(add))                                   # 2. contar por IP
print("Peticiones por IP:", sorted(peticiones.collect()))
frecuentes = peticiones.filter(lambda x: x[1] > 1)                     # 3. mas de 1
print("IPs con mas de 1 peticion:", frecuentes.collect())
if os.path.exists("output/ips_frecuentes"):
    shutil.rmtree("output/ips_frecuentes")
frecuentes.saveAsTextFile("output/ips_frecuentes")                     # 4. guardar
print("Resultado guardado en output/ips_frecuentes/")
for f in sorted(os.listdir("output/ips_frecuentes")):
    if f.startswith("part-"):
        print(f, "->", open(f"output/ips_frecuentes/{f}").read().strip())

# ---------------- Ejercicio 6: Pipeline y Shuffle ----------------
titulo("Ejercicio 6: Pipeline y Shuffle")
v = sc.parallelize([("Madrid", 100), ("Barcelona", 200), ("Madrid", 50), ("Valencia", 50)])
resultado = (v.filter(lambda x: x[1] >= 60)          # se descartan las ventas menores de 60
              .reduceByKey(add)                      # Wide: Shuffle
              .sortBy(lambda x: x[1], ascending=False))
print("Resultado final:", resultado.collect())
print("Linaje (DAG):")
print(resultado.toDebugString().decode("utf-8"))

# ---------------- Ejercicio 7: map vs flatMap ----------------
titulo("Ejercicio 7: Diferencia Map vs FlatMap")
frases = sc.parallelize(["Hola mundo", "Apache Spark es guay"])
con_map = frases.map(lambda f: f.split(" "))
con_flat = frases.flatMap(lambda f: f.split(" "))
print("map     :", con_map.collect())
print("flatMap :", con_flat.collect())
print("Con map se obtiene una lista de listas (1 entrada -> 1 salida).")
print("Con flatMap se obtiene una lista plana (1 entrada -> N salidas).")

# ---------------- Ejercicio 8: Optimizacion ----------------
titulo("Ejercicio 8: Optimizacion")
print("Respuesta: si un RDD se usa 5 veces, hay que persistirlo con cache() o")
print("persist(StorageLevel.MEMORY_AND_DISK). Asi Spark no recalcula todo el linaje")
print("en cada accion. Al terminar se libera con unpersist().")
rdd = sc.parallelize(range(1000000), 4).map(lambda x: x * 2).filter(lambda x: x % 3 == 0)
rdd.persist(StorageLevel.MEMORY_AND_DISK)
print("Count 1:", rdd.count())
print("Suma   :", rdd.sum())
print("Max    :", rdd.max())
print("Min    :", rdd.min())
print("Count 2:", rdd.count())
print("Nivel de almacenamiento:", rdd.getStorageLevel())
rdd.unpersist()

spark.stop()
