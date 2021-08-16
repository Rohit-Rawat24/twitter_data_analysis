#!/usr/bin/env python
# coding: utf-8

# In[12]:


import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate();
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark


# In[11]:


#sc.stop()


# In[3]:


import os
import tweepy as tw


# In[5]:


# API Keys and Tokens
consumer_key = 'TXq9cSiPgoZvcKFb2h1wQcnfF'
consumer_secret = 'Hf3jBUCLDd065ndxrPqhElNCdq4PUYuC0zh0MAyLOAc5vICA7C'
access_token = '1423514561668272128-Exw0A7NmUy6txQg2JIOCzmATbvZeJY'
access_secret = 'GyJnh9o1iCDbtDHtJXXuGPqjtDZ4OxRgoy7W7gC4sPeLx'


# In[6]:


auth = tw.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_secret)
api= tw.API(auth,wait_on_rate_limit =True)


# In[7]:


tweets = tw.Cursor(api.search, q="#Tokyo2020", since = "2021-07-23", until = "2021-08-08",lang="en").items(1)
[[tweet] for tweet in tweets]


# In[8]:


tweets = tw.Cursor(api.search, q="#Tokyo2020", since = "2021-07-23", until = "2021-08-08",lang="en").items(500)
users_details = [[tweet.text,tweet.user.screen_name,tweet.user.name] for tweet in tweets]
users_details


# In[9]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([     StructField("Tweet",StringType(),True),     StructField("Username",StringType(),True),     StructField("Name", StringType(), True),   ])


# In[13]:


df = spark.createDataFrame(data=users_details,schema=schema)
df.show(500)


# In[ ]:




