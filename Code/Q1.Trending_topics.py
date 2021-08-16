#!/usr/bin/env python
# coding: utf-8

# In[25]:


import tweepy as tw
import os
import json
import sys


# In[13]:


# API Keys and Tokens
consumer_key = 'TXq9cSiPgoZvcKFb2h1wQcnfF'
consumer_secret = 'Hf3jBUCLDd065ndxrPqhElNCdq4PUYuC0zh0MAyLOAc5vICA7C'
access_token = '1423514561668272128-Exw0A7NmUy6txQg2JIOCzmATbvZeJY'
access_secret = 'GyJnh9o1iCDbtDHtJXXuGPqjtDZ4OxRgoy7W7gC4sPeLx'


# In[14]:


# Authorization and Authentication
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tw.API(auth)


# In[15]:


# WOEID of delhi, india
woeid = 20070458


# In[16]:


# fetching the trends
Trends = api.trends_place(id = woeid)


# In[17]:


for value in Trends:
     print(value)


# In[27]:


import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate();
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark


# In[23]:


#sc.stop()


# In[19]:


for value in Trends:
    rdd = spark.sparkContext.parallelize(value['trends'])
Trend_with_hashtag=rdd.toDF(sampleRatio=0.01)


# In[20]:


Trend_with_hashtag.createOrReplaceTempView("Trend")
TD=spark.sql("select name, tweet_volume from Trend")
TD.show(50)


# In[21]:


trends = api.trends_place(id = woeid,exclude = "hashtags")
for value in trends:
    print(value)


# In[28]:


for value in trends:
    RDD = spark.sparkContext.parallelize(value.get('trends'))
trend_without_hashtag=RDD.toDF(sampleRatio=0.01)


# In[29]:


trend_without_hashtag.createOrReplaceTempView("trend")
td=spark.sql("select name, tweet_volume from trend")
td.show(50)


# In[ ]:




