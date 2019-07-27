
# coding: utf-8

# In[2]:


import IPython


# In[ ]:
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import numpy as np
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import matplotlib as mpl
try:
    sc.stop()
except:
    pass
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("Final")


sc=SparkContext(conf = conf)
spark = SparkSession(sparkContext=sc)


# In[ ]:


#read the data
spark = SparkSession.builder.appName('aggs').getOrCreate()


# In[ ]:


df = spark.read.option("sep","\t").csv('gs://dataproc-1470d5bd-a7d1-4b38-ad99-e058d7d6499e-asia-southeast1/tsv/*.tsv.gz', inferSchema=True, header=True)
#sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", AKIAJKEPEH26P6HRZF6Q )
#sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", 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)
#df=spark.read.option("sep","\t").csv('s3a://amazon-reviews-pds/parquet/*.tsv.gz')


# In[ ]:


df.count()


# In[ ]:


df.show(10)


# In[ ]:


df1=df.select(['customer_id', 'helpful_votes','verified_purchase'])
df1.show()


# In[ ]:


#The Top 10 helpful Customers
df1.orderBy(df['helpful_votes'].desc()).show()


# In[ ]:


# The customer rating 
df2=df.select(['customer_id', 'star_rating','verified_purchase'])
df2.show()


# In[ ]:


# The customer rating with verified
df2.createOrReplaceTempView('Cumstomer_rating')
df3=spark.sql("SELECT * FROM Cumstomer_rating where verified_purchase == 'Y'")
df3.show()


# In[ ]:


# The customer rating with verified 
YTotal=df3.count()
print "Total number of Customer who vote with verified",YTotal
Y5star=df3.filter(df['star_rating'] == 5).count()
print "Total number of Customer who vote 5 star with verified",Y5star
Y4star=df3.filter(df['star_rating'] == 4).count()
print "Total number of Customer who vote 4 star with verified",Y4star
Y3star=df3.filter(df['star_rating'] == 3).count()
print "Total number of Customer who vote 3 star with verified",Y3star
Y2star=df3.filter(df['star_rating'] == 2).count()
print "Total number of Customer who vote 2 star with verified",Y2star
Y1star=df3.filter(df['star_rating'] == 1).count()
print "Total number of Customer who vote 1 star with verified",Y1star


# In[39]:


# The customer rating with unverified
df4=df2.filter(df['verified_purchase'] == 'N')
df4.show()


# In[40]:


# The customer rating with verified 
NTotal=df4.count()
print "Total number of Customer who vote with unverified",NTotal
N5star=df4.filter(df['star_rating'] == 5).count()
print "Total number of Customer who vote 5 star with unverified",N5star
N4star=df4.filter(df['star_rating'] == 4).count()
print "Total number of Customer who vote 4 star with unverified",N4star
N3star=df4.filter(df['star_rating'] == 3).count()
print "Total number of Customer who vote 3 star with unverified",N3star
N2star=df4.filter(df['star_rating'] == 2).count()
print "Total number of Customer who vote 2 star with unverified",N2star
N1star=df4.filter(df['star_rating'] == 1).count()
print "Total number of Customer who vote 1 star with unverified",N1star


# In[54]:


def draw_pie(labels,quants):  
    # make a square figure    
    plt.figure(1, figsize=(6,6))
    # For China, make the piece explode a bit    
    expl = [0,0,0,0,0,0,0,0,0,0]
    # Colors used. Recycle if not enough.    
    colors  = ["lightskyblue","red","coral","lightgreen","yellow","orange"]       
    plt.pie(quants, explode=expl, colors=colors, labels=labels, autopct='%1.1f%%',pctdistance=0.8, shadow=True)  
    plt.title('Customer star rating ', bbox={'facecolor':'0.8', 'pad':5})   
    #plt.show()  
    plt.savefig("/home/jinj/pie.jpg")    
    plt.close() 
    # labels: Each rating with verified/unverified
     # quants: number of each rating
labels   = ['5star(Y)', '4star(Y)', '3star(Y)', '2star(Y)', '1star(Y)', '5star(N)', 'N4star(N)', 'N3star(N)', 'N2star(N)', 'N1star(N)'] 
quants   = [Y5star, Y4star, Y3star, Y2star, Y1star, N5star, N4star, N3star, N2star, N1star] 
draw_pie(labels,quants)

