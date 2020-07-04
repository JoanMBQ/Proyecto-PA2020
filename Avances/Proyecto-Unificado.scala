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

// MAGIC %md
// MAGIC ### DATAFRAME DE LA PROVINCIA DE LOJA

// COMMAND ----------

val dataLoja = data.where($"provincia" === "11")
dataLoja.count

// COMMAND ----------

// MAGIC %md
// MAGIC ##Preguntas - Segundo Entregable

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1) ¿Cuál es la cantidad de personas por provincia que tienen la edad mínima y tienen un empleo no remunerado? y ¿cuál es el total de todas las provincias?

// COMMAND ----------

dataProvincias.where($"edad" === 15).where($"condicion_actividad" === "5 - Empleo no remunerado").groupBy("provincia").count().sort($"count".desc).show(24, false)

// COMMAND ----------

dataProvincias.where($"edad" === 15).where($"condicion_actividad" === "5 - Empleo no remunerado").count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2) ¿Cuál es la cantidad de personas por provincia según el siguiente rango de edades: 15-30/31-50/51-70/70+?

// COMMAND ----------

dataProvincias.where($"edad" >= 15).where($"edad" <= 30).groupBy("provincia").count().sort($"count".desc).show(24, false)

// COMMAND ----------

dataProvincias.where($"edad" >= 31).where($"edad" <= 50).groupBy("provincia").count().sort($"count".desc).show(24, false)

// COMMAND ----------

dataProvincias.where($"edad" >= 51).where($"edad" <= 70).groupBy("provincia").count().sort($"count".desc).show(24, false)

// COMMAND ----------

dataProvincias.where($"edad" >= 71).groupBy("provincia").count().sort($"count".desc).show(24, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3) ¿En qué provincia existe mayor cantidad de empleados que están en centro de alfabetización?

// COMMAND ----------

dataProvincias.where($"nivel_de_instruccion" === "02 - Centro de alfabetización").groupBy("provincia").count().sort(desc("count")).show(false)

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

// MAGIC %md
// MAGIC ### 5) ¿Cuál es la provincia con el total más alto de ingreso laboral de sus empleados?

// COMMAND ----------

dataProvincias.groupBy("provincia").agg(sum("ingreso_laboral") as ("Total de ingresos")).sort(desc("Total de ingresos")).show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6) ¿Cuántas personas económicamente activas por provincia están en la condición de desempleo abierto?

// COMMAND ----------

dataProvincias.where($"condicion_actividad" === "7 - Desempleo abierto").groupBy("provincia").count().sort($"count".desc).show(24, false);

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7) ¿Cuántas personas económicamente activas por provincia están en la condición de empleo adecuado/pleno?

// COMMAND ----------

dataProvincias.where($"condicion_actividad" === "1 - Empleo Adecuado/Pleno").groupBy("provincia").count().sort($"count".desc).show(24, false);

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8) ¿Cuál es la tasa de empleo en los diferentes sectores en la provincia de Loja?

// COMMAND ----------

println((f"${(dataLoja.where($"sectorizacion" === "1 - Sector Formal").count / dataLoja.count.toDouble) *100}%.3f%% En la provincia de Loja trabajan en el sector Formal"))
println((f"${(dataLoja.where($"sectorizacion" === "2 - Sector Informal").count / dataLoja.count.toDouble) *100}%.3f%% En la provincia de Loja trabajan en el sector Informal"))
println((f"${(dataLoja.where($"sectorizacion" === "3 - Empleo Doméstico").count / dataLoja.count.toDouble) *100}%.3f%% En la provincia de Loja trabajan en el sector Domestico"))
println((f"${(dataLoja.where($"sectorizacion" === "4 - No Clasificados por Sector").count / dataLoja.count.toDouble) *100}%.3f%% En la provincia de Loja trabajan en un sector No clasificado"))
println((f"${(dataLoja.where($"sectorizacion".isNull).count / dataLoja.count.toDouble) *100}%.3f%% En la provincia de Loja No tienen completa esta información"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9) ¿Cuáles son los ingresos maximos por ocupación en la provincia de Loja?

// COMMAND ----------

dataLoja.groupBy("grupo_ocupacion").agg(max("ingreso_laboral").as("Ingresos maximos por ocupación")).sort($"Ingresos maximos por ocupación".desc).show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10) ¿Cuál es la provincia con mayor actividad en A. Agricultura, ganadería caza y silvicultura y pesca?

// COMMAND ----------

dataProvincias.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca").groupBy("provincia").count().sort($"count".desc).show(1, false);
