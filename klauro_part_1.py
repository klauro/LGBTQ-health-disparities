#!/usr/bin/env python
# coding: utf-8

# In[1]:


#sc.stop()
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
sqlContext = SQLContext(sc)


# In[2]:


conf = SparkConf().setAppName('LGBT_health').set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext.getOrCreate(conf=conf)


# In[3]:


nchat = spark.read.option("inferSchema","true").option("sep","\t").option("header", "true").csv("NCHAT_Data.tsv")
                    
lgbt = spark.read.option("inferSchema","true").option("sep","\t").option("header", "true").csv("LGBT_Equality.tsv")

insurance = spark.read.option("header", True).csv("/home/klauro/insurance.csv")


# In[4]:


print(nchat.count())
print(lgbt.count())
print(insurance.count())


# In[6]:


nchat.registerTempTable("nchat")
lgbt.registerTempTable("lgbt")
insurance.registerTempTable("insurance")


# In[7]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[8]:


from pyspark.sql.functions import col,when
from pyspark.sql.functions import regexp_replace


# In[13]:


refinednchat = sqlContext.sql("SELECT AGE_SD AS Age, HHR4 AS sex, STRESS4_G AS LGBTQ_stress, HHR2 AS relationship_stat, Q46A AS health_stat, STRESS3_J AS insurance, Q53B AS health_identity, Q53D AS trust_provider, Q53E AS health_seriously FROM nchat")
refinednchat.filter((refinednchat.Age != '-98') & (refinednchat.LGBTQ_stress != '-99') & (refinednchat.LGBTQ_stress != '99') & (refinednchat.LGBTQ_stress != '-98') & (refinednchat.health_stat != '-99') & (refinednchat.health_stat != '-98'))
#print(refinednchat.count())
refinednchat = refinednchat.withColumn("sex",regexp_replace("sex","1","male"))
refinednchat = refinednchat.withColumn("sex",regexp_replace("sex","2","female"))
refinednchat = refinednchat.withColumn("sex",regexp_replace("sex","3","n/a"))
#refinednchat.show(5)
refinednchat.registerTempTable("nchat_df")


# In[14]:


refinedlgbt = sqlContext.sql("SELECT V030 AS region, V012 AS 2020_health, V025 AS GI_healthcare, V029 AS No_LGBT, V077 AS SO_ND_private, V082 AS GI_medicaid_trans FROM lgbt WHERE V029 !=-1")

refinedlgbt = refinedlgbt.withColumn("region",regexp_replace("region","0","n/a"))
refinedlgbt = refinedlgbt.withColumn("region",regexp_replace("region","1","midwest"))
refinedlgbt = refinedlgbt.withColumn("region",regexp_replace("region","2","northeast"))
refinedlgbt = refinedlgbt.withColumn("region",regexp_replace("region","3","south"))
refinedlgbt = refinedlgbt.withColumn("region",regexp_replace("region","4","west"))
#refinedlgbt.show(5)
refinedlgbt.registerTempTable("lgbt_df")


# In[15]:


insurance = insurance.withColumn("region",regexp_replace("region","southeast","south"))
insurance = insurance.withColumn("region",regexp_replace("region","southwest","west"))
insurance = insurance.withColumn("region",regexp_replace("region","northwest","west"))
#insurance.show(5)


# In[78]:


ins_lgbt = sqlContext.sql("SELECT /*+ BROADCAST(lgbt_df)*/ region, sex, ROUND(CAST(charges as int)) AS charges, 2020_health, No_LGBT FROM insurance LEFT JOIN lgbt_df USING (region)")
#ins_lgbt = sqlContext.sql("SELECT lgbt_df.region, 2020_health, No_LGBT, age, sex FROM lgbt_df LEFT JOIN insurance ON lgbt_df.region=insurance.region")
ins_lgbt.show()
#print(ins_lgbt.count())
ins_lgbt.registerTempTable('ins_lgbt')


# In[86]:


full_data = sqlContext.sql("SELECT /*+ BROADCAST(nchat_df)*/ * FROM ins_lgbt LEFT JOIN nchat_df USING (sex)")
#full_data = sqlContext.sql("SELECT LGBTQ_stress, health_stat, health_seriously, region, 2020_health, No_LGBT, ins_lgbt.age, nchat_df.sex FROM nchat_df LEFT JOIN ins_lgbt ON nchat_df.sex=ins_lgbt.sex WHERE LGBTQ_stress !=99")
print(full_data.count())
full_data = full_data.where("2020_health is not null")
full_data.show()
full_data.registerTempTable("full_data")

#full_data.write.option("header", "true").csv("lgbt_health_data.csv")
#full_data.write.csv("lgbt_health_data_only.csv")


# In[94]:


input_path = "/home/klauro/lgbt_health_dataonly.csv"
input_file = sc.textFile(input_path)


# In[96]:


lgbt_health = sqlContext.sql("SELECT 2020_health AS num_health_policies, ROUND(AVG(CAST(health_seriously as int)),2) AS avg_perception_health_taken_seriously FROM full_data WHERE health_seriously !=-98 GROUP BY 2020_health ORDER BY 2020_health DESC")
lgbt_health.show()
#lgbt_health.write.csv("lgbt_health.csv")


# In[85]:


perceived_health = sqlContext.sql("SELECT LGBTQ_stress, health_stat, health_seriously FROM full_data WHERE LGBTQ_stress != -98")

perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","1","Poor"))
perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","2","Fair"))
perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","3","Good"))
perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","4","Very Good"))
perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","5","Excellent"))
perceived_health = perceived_health.withColumn("health_stat",regexp_replace("health_stat","-98","Refusal"))

perceived_health.registerTempTable('perceived_health')


# In[185]:


stress_health = sqlContext.sql("SELECT health_stat AS perceived_health_status, ROUND(AVG(CAST(LGBTQ_stress as int)),1) AS avg_lgbt_stress FROM perceived_health WHERE LGBTQ_stress != 99 GROUP BY health_stat ORDER BY avg_lgbt_stress DESC")
stress_health.registerTempTable("stress_health")
stress_health.show()
stress_health.write.csv("stress_health.csv")


# In[186]:


healthvsidentity = sqlContext.sql("SELECT health_seriously AS Concern_healthcare_due_to_identity, ROUND(AVG(CAST(LGBTQ_stress as int)),1) AS avg_lgbt_stress FROM perceived_health WHERE health_seriously !=-98 GROUP BY Concern_healthcare_due_to_identity ORDER BY avg_lgbt_stress DESC")
healthvsidentity.show()
healthvsidentity.write.csv("healthvsidentity.csv")


# In[ ]:




