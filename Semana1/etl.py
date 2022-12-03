from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)


#Creo Dataframe de los vuelos 2021 y 2022
df_2021 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv", sep=";")
df_2021 = df_2021.withColumn('fecha', to_date(df_2021.Fecha, 'dd/MM/yyyy'))\
          .withColumn('pasajeros', df_2021.Pasajeros.cast('int'))\
          .withColumnRenamed('Hora UTC', 'horaUTC')\
          .withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo')\
          .withColumnRenamed('Clasificación Vuelo', 'clasificacion_vuelo')\
          .withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento')\
          .withColumnRenamed('Aeropuerto', 'aeropuerto')\
          .withColumnRenamed('Origen / Destino', 'origen_destino')\
          .withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre')\
          .withColumnRenamed('Aeronave', 'aeronave')
df_2021.printSchema()

df_2022 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv", sep = ";")
df_2022 = df_2022.withColumn('fecha',to_date(df_2022.Fecha, 'dd/MM/yyyy'))\
          .withColumn('pasajeros', df_2022.Pasajeros.cast('int'))\
          .withColumnRenamed('Hora UTC', 'horaUTC')\
          .withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo')\
          .withColumnRenamed('Clasificación Vuelo', 'clasificacion_vuelo')\
          .withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento')\
          .withColumnRenamed('Aeropuerto', 'aeropuerto')\
          .withColumnRenamed('Origen / Destino', 'origen_destino')\
          .withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre')\
          .withColumnRenamed('Aeronave', 'aeronave')

df_2022.printSchema()

#Genero un Datafrane con los datos de Aeropuertos
df_aeropuertos = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv", sep = ";")
df_aeropuertos = df_aeropuertos.withColumn('elev', df_aeropuertos.elev.cast('float'))\
                .withColumn('distancia_ref', df_aeropuertos.distancia_ref.cast('float'))\
                .withColumnRenamed('local','aeropuerto')\
                .withColumnRenamed('oaci','oac')
df_aeropuertos.printSchema()
##opcional: si queres ver la data que esta en el DF### 



##ETL
#Uno los dataframes 2021 y 2022
df_fechas = df_2021.union(df_2022)
df_fechas = df_fechas.drop(col('Calidad dato')).na.fill(0,["pasajeros"])

df_fechas.printSchema()

#Filtro el dataset de fechas solo por los vuelos domesticos
vuelos_domesticos = df_fechas.filter(df_fechas.clasificacion_vuelo == 'Domestico')
vuelos_domesticos.show(5)

#Dropeo columnas en datos de aeropuertos que son innecesarias
df_aeropuertos = df_aeropuertos.drop(col('inhab')).drop(col('fir')).na.fill(0, ["distancia_ref"])
df_aeropuertos.printSchema()

#Genero una vista de los vuelos y realizo las queries solicitadas
vuelos_domesticos.createOrReplaceTempView('vuelos')
vuelos2021a2022 = spark.sql("SELECT count(*) as cantidad_vuelos FROM vuelos WHERE fecha BETWEEN '2021-12-01' AND '2022-01-31'")
vuelos2021a2022.show()

pasajeros_aerolineas = spark.sql("SELECT sum(pasajeros) as cantidad_pasajeros FROM vuelos \
                                  WHERE aerolinea_nombre == 'AEROLINEAS ARGENTINAS SA' \
                                  AND fecha BETWEEN '2021-12-01' AND '2022-06-30'")
pasajeros_aerolineas.show()

#Genero vista del dataframe de aeropuertos para enriquecer los datos de los vuelos
df_aeropuertos.createOrReplaceTempView('aeropuertos')

vuelos = spark.sql("SELECT v.fecha, v.horaUTC, v.aeropuerto as aep_salida, a.ref as ciudad_salida, v.origen_destino as aep_destino, b.ref as ciudad_destino, v.pasajeros \
                    FROM vuelos v \
                    JOIN aeropuertos a ON v.aeropuerto = a.aeropuerto \
                    JOIN aeropuertos b ON v.origen_destino = b.aeropuerto \
                    WHERE v.fecha BETWEEN '2021-12-01' AND '2022-06-30' ORDER BY fecha DESC")
vuelos.show(5)

#Genero un dataframe filtrado por los vuelos que solamente salen de la ciudad de Buenos Aires
buenos_aires = vuelos.filter(vuelos.ciudad_salida == 'Ciudad de Buenos Aires').groupBy(vuelos.aep_salida).count()                                                                                                             
buenos_aires.show()

#Realizo una query filtrando la cantidad de pasajeros por vuelo, armando un ranking de los 10 vuelos con más pasajeros
charged_aerolineas = spark.sql("SELECT aerolinea_nombre, sum(pasajeros) as cant_pasajeros \
                    FROM vuelos \
                    WHERE fecha BETWEEN '2021-12-01' AND '2022-06-30' AND aerolinea_nombre NOT IN (0)\
                    GROUP BY aerolinea_nombre \
                    ORDER BY cant_pasajeros DESC")
charged_aerolineas.show(10)

#Realizo un filtro por la cantidad de aeronaves más utilizadas en los vuelos que tienen como origen o destino a Buenos Aires
aeronaves = spark.sql("SELECT v.aeronave\
                       FROM vuelos v\
                       JOIN aeropuertos a ON a.aeropuerto == v.aeropuerto \
                       WHERE a.provincia IN ('CIUDAD AUTÓNOMA DE BUENOS AIRES', 'BUENOS AIRES') AND v.fecha BETWEEN '2021-12-01' AND '2022-06-30' \
                       AND v.tipo_de_movimiento == 'Despegue' \
                       AND v.aeronave NOT IN (0)\
                       GROUP BY aeronave \
                       ORDER BY v.aeronave")
aeronaves.show(10)

df_fechas.createOrReplaceTempView('fechas')

hc.sql("insert into examen_final.vuelos select * from fechas;")
hc.sql("insert into examen_final.aeropuertos select * from aeropuertos;")



