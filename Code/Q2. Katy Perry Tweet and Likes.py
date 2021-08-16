#!/usr/bin/env python
# coding: utf-8

# In[11]:


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


sc.stop()


# In[6]:


import tweepy

# API Keys and Tokens
consumer_key = 'TXq9cSiPgoZvcKFb2h1wQcnfF'
consumer_secret = 'Hf3jBUCLDd065ndxrPqhElNCdq4PUYuC0zh0MAyLOAc5vICA7C'
access_key = '1423514561668272128-Exw0A7NmUy6txQg2JIOCzmATbvZeJY'
access_secret = 'GyJnh9o1iCDbtDHtJXXuGPqjtDZ4OxRgoy7W7gC4sPeLx'

  


# In[7]:


# Authorization to consumer key and consumer secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  
# Access to user's access key and access secret
auth.set_access_token(access_key, access_secret)
  
# Calling api
api = tweepy.API(auth)


# In[8]:


# 200 tweets to be extracted
number_of_tweets=200
tweets = api.user_timeline(screen_name='katyperry',count=number_of_tweets)

#Status
for i in tweets:
    print(i)

#Convert the status into list of list
ls=[[i.text,i.favorite_count,i.retweet_count,i.created_at] for i in tweets]


# In[9]:


ls


# In[12]:


#Creating the spark dataframe
columns=['Tweet','Likes','Retweets','Tweet_time']
dataframe = spark.createDataFrame(ls,columns)

#Dataframe
dataframe.show(200)


# In[ ]:





# In[ ]:




