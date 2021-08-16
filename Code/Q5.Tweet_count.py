#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import pyspark.sql
from pyspark import SparkContext
sc = SparkContext.getOrCreate();
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark


# In[3]:


data1=spark.read.format("com.databricks.spark.csv").options(inferSchema="true",header='true',escape='"').load("gender-classifier-DFE-791531.csv")


# In[4]:


data1.show()


# In[5]:


data1.createOrReplaceTempView('data')
spark.sql("select * from data order by tweet_count desc")


# In[6]:


data1.select('name','tweet_count').distinct().where(data1.tweet_count>100000).sort(data1.tweet_count.desc()).show()


# In[ ]:




