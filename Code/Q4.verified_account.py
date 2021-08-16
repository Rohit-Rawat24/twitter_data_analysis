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


# In[2]:


import os
import tweepy as tw


# In[11]:


#sc.stop()


# In[4]:


# API Keys and Tokens
consumer_key = 'TXq9cSiPgoZvcKFb2h1wQcnfF'
consumer_secret = 'Hf3jBUCLDd065ndxrPqhElNCdq4PUYuC0zh0MAyLOAc5vICA7C'
access_token = '1423514561668272128-Exw0A7NmUy6txQg2JIOCzmATbvZeJY'
access_secret = 'GyJnh9o1iCDbtDHtJXXuGPqjtDZ4OxRgoy7W7gC4sPeLx'


# In[5]:


auth = tw.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_secret)
api= tw.API(auth,wait_on_rate_limit =True)


# In[6]:


search_words = "#olympic"
date_since = "2021-08-07"


# In[7]:


tweets = tw.Cursor(api.search,q=search_words,lang="en",since=date_since).items(200)
#tweets = tw.Cursor(api.search, lang="en",since=date_since).items(20)
[tweet.text for tweet in tweets]


# In[8]:


new_search= search_words 
new_search


# In[9]:



tweets = tw.Cursor(api.search, q=new_search,lang="en",since=date_since).items(500)
users_details = [[tweet.user.screen_name,tweet.user.name,tweet.user.followers_count,tweet.user.verified]for tweet in tweets]
users_details


# In[13]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([     StructField("Username",StringType(),True),     StructField("Name",StringType(),True),     StructField("Followers_count", IntegerType(), True),     StructField("Verified", StringType(), True)   ])
 
df = spark.createDataFrame(data=users_details,schema=schema)
#df.printSchema()
#df.show(truncate=False)
df.where(df.Verified == "true").show()


# In[ ]:





# In[ ]:




