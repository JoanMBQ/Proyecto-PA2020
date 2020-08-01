// Databricks notebook source
// MAGIC %md
// MAGIC ## CREACIÓN DEL ESQUEMA DEL DATASET

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameNaFunctions
val dataSchema = StructType(
	Array(
		StructField("id_persona", DecimalType(26,0), true),
		StructField("anio", IntegerType, true),
		StructField("mes", IntegerType, true),
		StructField("provincia", StringType, true),
		StructField("canton", IntegerType, true),
		StructField("area", StringType, true),
		StructField("genero", StringType, true),
		StructField("edad", IntegerType, true),
		StructField("estado_civil", StringType, true),
		StructField("nivel_de_instruccion", StringType, true),
		StructField("etnia", StringType, true),
		StructField("ingreso_laboral", IntegerType, true),
		StructField("condicion_actividad", StringType, true),
		StructField("sectorizacion", StringType, true),
		StructField("grupo_ocupacion", StringType, true),
		StructField("rama_actividad", StringType, true),
		StructField("factor_expansion", DoubleType, true)
	));

// COMMAND ----------

// MAGIC %md
// MAGIC ## IMPORTACIÓN DEL DATASET

// COMMAND ----------

val data = spark.read
	.schema(dataSchema)
	.option("header","true")
	.option("delimiter","\t")
	.csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

// MAGIC %md
// MAGIC ## CAMBIOS EN EL DATAFRAME

// COMMAND ----------

// MAGIC %md
// MAGIC IDs de las provincias del Ecuador obtenidos de: http://web.educacion.gob.ec/CNIE/pdf/Anexo%20con%20Codificacion.pdf

// COMMAND ----------

val dataProvincias = data.na.replace("provincia", Map(
	"01" -> "Azuay",
	"02" -> "Bolivar",
	"03" -> "Cañar",
	"04" -> "Carchi",
	"05" -> "Cotopaxi",
	"06" -> "Chimborazo",
	"07" -> "El Oro",
	"08" -> "Esmeraldas",
	"09" -> "Guayas",
	"10" -> "Imbabura",
	"11" -> "Loja",
	"12" -> "Los Rios",
	"13" -> "Manabi",
	"14" -> "Morona Santiago",
	"15" -> "Napo",
	"16" -> "Pastaza",
	"17" -> "Pichincha",
	"18" -> "Tungurahua",
	"19" -> "Zamora Chinchipe",
	"20" -> "Galapagos",
	"21" -> "Sucumbios",
	"22" -> "Orellana",
	"23" -> "Santo Domingo de los Tsachilas",
	"24" -> "Santa Elena"
));

// COMMAND ----------

val schemaCantones = StructType(
	Array(
		StructField("idCanton", IntegerType, true),
		StructField("cantones", StringType, true)
      ));

// COMMAND ----------

val cantones = spark
    .read
    .schema(schemaCantones)
	.option("header","true")
	.option("delimiter",";")
    .option("encoding", "ISO-8859-1")
	.csv("/FileStore/tables/Cantones.csv");

// COMMAND ----------

val dataCantones = dataProvincias.join(cantones, dataProvincias("canton") === cantones("idCanton"), "inner")

// COMMAND ----------

val dataFinal = dataCantones.drop("canton").drop("idCanton")

// COMMAND ----------

dataFinal.createOrReplaceTempView("EDU_TABLE")

// COMMAND ----------

// MAGIC %md
// MAGIC ### DATAFRAME DE LA PROVINCIA DE LOJA

// COMMAND ----------

val dataLoja = dataFinal.where($"provincia" === "Loja")
dataLoja.count

// COMMAND ----------

// MAGIC %md
// MAGIC ##Preguntas - Tercer Entregable

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1) ¿Cuál es la cantidad de personas por provincia que tienen la edad mínima y tienen un empleo no remunerado? y ¿cuál es el total de todas las provincias?

// COMMAND ----------

display(dataFinal.where($"edad" === 15).where($"condicion_actividad" === "5 - Empleo no remunerado").groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

dataProvincias.where($"edad" === 15).where($"condicion_actividad" === "5 - Empleo no remunerado").count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2) ¿Cuál es la cantidad de personas por provincia según el siguiente rango de edades: 15-30/31-50/51-70/70+?

// COMMAND ----------

display(dataFinal.where($"edad" >= 15).where($"edad" <= 30).groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

display(dataFinal.where($"edad" >= 31).where($"edad" <= 50).groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

display(dataFinal.where($"edad" >= 51).where($"edad" <= 70).groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

display(dataFinal.where($"edad" >= 71).groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3) ¿En qué provincia existe mayor cantidad de empleados que están en centro de alfabetización?

// COMMAND ----------

display(dataFinal.where($"nivel_de_instruccion" === "02 - Centro de alfabetización").groupBy("provincia").count().sort(desc("count")))

// COMMAND ----------

display(dataProvincias.where($"nivel_de_instruccion" === "02 - Centro de alfabetización").groupBy("anio").pivot("provincia").count().orderBy("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4) ¿Cuál es el porcentaje de cada etnia en la provincia de Loja?

// COMMAND ----------

val dataLoja = data.where($"provincia" === "11")
val pIndigena = (dataLoja.where($"etnia" === "1 - Indígena").count / dataLoja.count.toDouble)*100
val pAfroecuatoriano = (dataLoja.where($"etnia" === "2 - Afroecuatoriano").count / dataLoja.count.toDouble)*100
val pNegro = (dataLoja.where($"etnia" === "3 - Negro").count / dataLoja.count.toDouble)*100
val pMulato = (dataLoja.where($"etnia" === "4 - Mulato").count / dataLoja.count.toDouble)*100
val pMontubio = (dataLoja.where($"etnia" === "5 - Montubio").count / dataLoja.count.toDouble)*100
val pMestizo = (dataLoja.where($"etnia" === "6 - Mestizo").count / dataLoja.count.toDouble)*100
val pBlanco = (dataLoja.where($"etnia" === "7 - Blanco").count / dataLoja.count.toDouble)*100
val pOtro = (dataLoja.where($"etnia" === "8 - Otro").count / dataLoja.count.toDouble)*100

// COMMAND ----------

import sqlContext.implicits._
val etniasLoja = Seq(
  ("Indigena", 2.55),
  ("Afroecuatoriano", 0.22),
  ("Negro", 0.52),
  ("Mulato", 0.25),
  ("Montubio", 0.18),
  ("Mestizo", 94.80),
  ("Blanco", 1.45),
  ("Otro", 0.00)
).toDF("etnia", "porcentaje")

// COMMAND ----------

display(etniasLoja)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5) ¿Cuál es la provincia con el total más alto de ingreso laboral de sus empleados?

// COMMAND ----------

display(dataFinal.groupBy("provincia").agg(sum("ingreso_laboral") as ("Total de ingresos")).sort(desc("Total de ingresos")))

// COMMAND ----------

display(dataFinal.groupBy("anio").pivot("provincia").agg(sum("ingreso_laboral") as ("Total de ingresos")).orderBy("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6) ¿Cúal es el porcentaje de personas en las diferentes condiciones de actividad en el Ecuador?

// COMMAND ----------

display(dataFinal.groupBy(col("anio").as("Año")).pivot("condicion_actividad").count().orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7) ¿Cuántas personas económicamente activas por provincia están en la condición de Desempleo?
// MAGIC Se desea conocer cuantas de las personas económicamente activas encuestadas se encontraban en la condicion de desempleo.

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '7 - Desempleo abierto'
    OR e.condicion_actividad = '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '7 - Desempleo abierto'
    OR e.condicion_actividad = '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '7 - Desempleo abierto'
    OR e.condicion_actividad = '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora los mostramos en un gráfico de barras agrupándolos por provincia, pero separandolos en los diferentes años (2015, 2016, 2017, 2018, 2019)

// COMMAND ----------

display(dataFinal
        .where(($"condicion_actividad" === "7 - Desempleo abierto") || ($"condicion_actividad" === "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"));

// COMMAND ----------

display(dataFinal
        .where(($"condicion_actividad" === "7 - Desempleo abierto") || ($"condicion_actividad" === "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"));

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8) ¿Cuántas personas económicamente activas por provincia están en la condición de Empleo Adecuado o Pleno?
// MAGIC Se desea conocer cuantas de las personas económicamente activas encuestadas se encontraban en la condicion de desempleo.

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad != '7 - Desempleo abierto'
    OR e.condicion_actividad != '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad != '7 - Desempleo abierto'
    OR e.condicion_actividad != '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad != '7 - Desempleo abierto'
    OR e.condicion_actividad != '8 - Desempleo oculto'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora los mostramos en un gráfico de barras agrupándolos por provincia, pero separandolos en los diferentes años (2015, 2016, 2017, 2018, 2019)

// COMMAND ----------

display(dataFinal
        .where(($"condicion_actividad" !== "7 - Desempleo abierto") || ($"condicion_actividad" !== "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"))

// COMMAND ----------

display(dataFinal
        .where(($"condicion_actividad" !== "7 - Desempleo abierto") || ($"condicion_actividad" !== "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.1) Cuantas de las personas con empleo tienen un empleo adecuado o pleno?
// MAGIC Tomando en cuenta los datos obtenidos anteriormente, se desea saber cuantas personas encuestadas se encontraban en la condición de trabajar en un empleo pleno.

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '1 - Empleo Adecuado/Pleno'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2015 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '1 - Empleo Adecuado/Pleno'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2017 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

display(spark.sql("""
    SELECT e.provincia AS Provincias, SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) AS N_Personas
    FROM EDU_TABLE e
    WHERE e.condicion_actividad = '1 - Empleo Adecuado/Pleno'
    GROUP BY e.provincia
    ORDER BY SUM(CASE WHEN e.anio = 2019 THEN 1 ELSE 0 END) DESC
"""));

// COMMAND ----------

// MAGIC %md
// MAGIC Posteriormente los mostramos en un gráfico de barras agrupándolos por provincia en cada año que se realizó la encuesta.

// COMMAND ----------

// DBTITLE 0,Untitled
display(dataFinal
        .where($"condicion_actividad" === "1 - Empleo Adecuado/Pleno")
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"))

// COMMAND ----------

display(dataFinal
        .where($"condicion_actividad" === "1 - Empleo Adecuado/Pleno")
        .groupBy(col("anio").as("Año"))
        .pivot("provincia").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9) ¿Cuál es el número de personas economicamente activas en los diferentes sectores en el Ecuador?

// COMMAND ----------

display(dataFinal.groupBy(col("anio").as("Año")).pivot("sectorizacion").count().orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC Una vez obtenido el número de cuantas personas trabajan en cada sector de empleo, se procede a saber a que porcentaje corresponden.

// COMMAND ----------

display(dataFinal
        .groupBy(col("anio").as("Año"))
        .pivot("sectorizacion").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10) ¿Cuál es el número de personas economicamente activas en los diferentes sectores en la provincia de Loja?

// COMMAND ----------

display(dataLoja
        .groupBy(col("anio").as("Año"))
        .pivot("sectorizacion").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC Una vez obtenido el número de cuantas personas trabajan en cada sector de empleo en la provincia de Loja, se procede a saber a que porcentaje corresponden.

// COMMAND ----------

display(dataLoja
        .groupBy(col("anio").as("Año"))
        .pivot("sectorizacion").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 11) Cual es la cantidad de hombres y mujeres en condición de desempleo en la provincia de Loja?

// COMMAND ----------

display(dataLoja.
        where(($"condicion_actividad" === "7 - Desempleo abierto") || ($"condicion_actividad" === "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("genero").count()
        .orderBy("Año"))

// COMMAND ----------

display(dataLoja
        .where(($"condicion_actividad" === "7 - Desempleo abierto") || ($"condicion_actividad" === "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("genero").count()
        .orderBy("Año"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12) Cual es la cantidad de hombres y mujeres en condición de empleo en la provincia de Loja?

// COMMAND ----------

display(dataLoja
        .where(($"condicion_actividad" !== "7 - Desempleo abierto") || ($"condicion_actividad" !== "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("genero").count()
        .orderBy("Año"))

// COMMAND ----------

display(dataLoja
        .where(($"condicion_actividad" !== "7 - Desempleo abierto") || ($"condicion_actividad" !== "8 - Desempleo oculto"))
        .groupBy(col("anio").as("Año"))
        .pivot("genero").count()
        .orderBy("Año"))
