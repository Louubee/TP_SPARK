from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType,DateType,FloatType,TimestampType,BooleanType
import pandas as pd
from kafka import KafkaProducer
import time
import json
from pyspark.sql import functions as F


# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("TP_DATA_INTEGRATION") \
    .getOrCreate()

# Schéma pour le fichier page_view
food_facilities_schema =  StructType([
    StructField("_id", IntegerType(), True),
    StructField("encounter", StringType(), True),
    StructField("id", StringType(), True),
    StructField("placard_st", IntegerType(), True),
    StructField("placard_desc", StringType(), True),
    StructField("facility_name", StringType(), True),
    StructField("bus_st_date", DateType(), True),
    StructField("category_cd", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("num", IntegerType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("inspect_dt", DateType(), True),
    StructField("start_time", StringType(), True),  # Ou TimestampType si vous combinez date et heure
    StructField("end_time", StringType(), True),    # Pareil pour end_time
    StructField("municipal", StringType(), True),
    StructField("ispt_purpose", IntegerType(), True),
    StructField("abrv", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("reispt_cd", IntegerType(), True),
    StructField("reispt_dt", DateType(), True),
    StructField("status", IntegerType(), True)
])

# Schéma pour le fichier blocklisted_pages
geocoded_schema = StructType([
    StructField("_id", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("facility_name", StringType(), True),
    StructField("num", IntegerType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("municipal", StringType(), True),
    StructField("category_cd", StringType(), True),
    StructField("description", StringType(), True),
    StructField("p_code", IntegerType(), True),
    StructField("fdo", DateType(), True),
    StructField("bus_st_date", DateType(), True),
    StructField("bus_cl_date", DateType(), True),
    StructField("seat_count", IntegerType(), True),
    StructField("noroom", IntegerType(), True),
    StructField("sq_feet", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("placard_st", IntegerType(), True),
    StructField("x", FloatType(), True),
    StructField("y", FloatType(), True),
    StructField("address", StringType(), True)
])


schema_df_alco = StructType([
    StructField("_id", IntegerType(), True),
    StructField("encounter", StringType(), True),
    StructField("id", StringType(), True),
    StructField("placard_st", IntegerType(), True),
    StructField("facility_name", StringType(), True),
    StructField("bus_st_date", DateType(), True),
    StructField("description", StringType(), True),
    StructField("description_new", StringType(), True),
    StructField("num", IntegerType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("inspect_dt", DateType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("municipal", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("low", BooleanType(), True),
    StructField("medium", BooleanType(), True),
    StructField("high", BooleanType(), True),
    StructField("url", StringType(), True)
])

# Charger les données de food_facilities
food_facilities_df = spark.read.csv("/home/lou/Data_Integration/TP_SPARK/Food_facility_restaurant_violations.csv", schema=food_facilities_schema, sep=",")
# food_facilities_df, city
food_facilities_df.show()
# Charger les données de geocoded_schema
geocoded_df= spark.read.csv("/home/lou/Data_Integration/TP_SPARK/geocoded_food_facilities.csv", schema=geocoded_schema, sep=",")
# description, city
geocoded_df.show()
# Charger les données de geocoded_schema
alco_df = spark.read.csv("/home/lou/Data_Integration/TP_SPARK/data/output_data.csv", schema=schema_df_alco, sep=",")

# OBJECTIF : Créer unne table contenant une ligne pour chaque restaurant avec des infos de base : villes, nb inspections, ... 


# Sélectionner uniquement les colonnes nécessaires dans alco_df
alco_df_filtered = alco_df.select("id","facility_name","description","description_new","zip","state",).distinct()


# Sélectionner uniquement les colonnes nécessaires dans geocoded_df
geocoded_df_filtered = geocoded_df.select("id", "city").distinct()

# Effectuer la jointure sur la colonne 'id' entre alco_df_filtered et geocoded_df_filtered pour ajouter la ville 
joined_df = alco_df_filtered.join(geocoded_df_filtered, on="id", how="left")

#Ajout du nombre d'inspections
food_facilities_df_filtered_id=food_facilities_df.select("id")
food_facilities_count=food_facilities_df_filtered_id.groupBy("id").count().withColumnRenamed('count', 'nb_inspections')

joined_df=joined_df.join(food_facilities_count,on="id",how="left")

# Exporter le DataFrame en fichier CSV
joined_df.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("/home/lou/Data_Integration/TP_SPARK/structured_table/restaurant.csv") 

# Création d'une table : une ligne pour chaque ville.
# Groupement par 'state' et comptage des inspections
inspections_by_state = joined_df.groupBy("city").sum("nb_inspections").withColumnRenamed("sum(nb_inspections)", "total_inspections")
# Afficher les résultats

inspections_by_state.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("/home/lou/Data_Integration/TP_SPARK/structured_table/city.csv") 

# Création d'une table : une ligne pour chaque type de restaurant.
inspections_by_restaurant = joined_df.groupBy("description").sum("nb_inspections").withColumnRenamed("sum(nb_inspections)", "total_inspections").withColumnRenamed("description", "type_restaurant")

# Afficher les résultats
inspections_by_restaurant.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("/home/lou/Data_Integration/TP_SPARK/structured_table/description.csv") 
