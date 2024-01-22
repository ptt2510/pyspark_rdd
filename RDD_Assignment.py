#!/usr/bin/env python
# coding: utf-8

# # PySpark RDD Assignment
# ## 21522557	Trần Thanh Sơn
# ## 21522678	Phạm Trung Tín
# ## 21522515	Nguyễn Việt Quang
# **Given two separate datasets of a sports complex with the following shemas:**

# ## cust.csv
# | Cust ID  | First Name | Last Name  | Age  | Profession  |
# |----------|------------|------------|------|-------------|
# | 4000001  | Kristina   | Chung      | 55   | Pilot       |
# | 4000002  | Paige      | Chen       | 74   | Teacher     |
# | 4000003  | Sherri     | Melton     | 34   | Firefighter |
# | **...**  |            |            |      |             |
# | 4000010  | Dolores    | McLaughlin | 60   | Writer      |

# ## trans240.csv
# | Trans ID  | Date        | Cust ID  | Cost   | Game               | Equipment                         | City        | State      | Mode   |
# |-----------|-------------|----------|--------|--------------------|-----------------------------------|-------------|------------|--------|
# | 0         | Jun-26-2011 | 4000001  | 40.33  | Exercise & Fitness | Cardio Machine Accessories        | Clarksville | Tennessee  | credit |
# | 1         | May-26-2011 | 4000002  | 198.44 | Exercise & Fitness | Weightlifting Gloves              | Long Beach  | California | credit |
# | 2         | Jan-06-2011 | 4000002  | 5.58   | Exercise & Fitness | Weightlifting Machine Accessories | Anaheim     | California | credit |
# | 3         | May-06-2011 | 4000003  | 198.19 | Gymnastics         | Gymnastics Rings                  | Milwaukee   | Wisconsin  | credit |
# | ...       |             |          |        |                    |                                   |             |            |        |
# | 239       | Sep-20-2011 | 4000007  | 249.09 | Outdoor Recreation | Camping & Backpacking & Hiking    | Hampton     | Virginia   | credit |

# # Load data from trans240.csv and cust.csv and perform the following queries:

# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


# In[3]:


transRDD =  spark.sparkContext.textFile("trans240.csv").map(lambda x: x.split(","))


# In[4]:


custRDD =  spark.sparkContext.textFile("cust.csv").map(lambda x: x.split(","))


# ## 1. For each month, show the number of distinct players, and the total cost, the results are sorted by month

# In[5]:


b1_1 = transRDD.map(lambda x: (x[1].split("-")[0], float(x[3]))).reduceByKey(lambda x, y: x + y)
b1_1.collect()


# In[6]:


b1_2 = transRDD.map(lambda x: (x[1].split("-")[0], x[2])).distinct().groupByKey().mapValues(len)
b1_2.collect()


# In[7]:


def month_to_number(month):
  months = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12
  }
  return months[month]

b1_join = b1_1.join(b1_2).map(lambda x: (month_to_number(x[0]), (x[0], x[1][1], x[1][0])))
b1_join.collect()


# In[8]:


b1 = b1_join.sortByKey().map(lambda x: x[1])
b1.collect()


# ## 2. For each month, show the name three youngest player, the results are sorted by the month

# In[9]:


b2_1 = transRDD.map(lambda x: (x[2], x[1].split('-')[0]))
b2_1.collect()


# In[10]:


b2_2 = custRDD.map(lambda x: (x[0], (x[1] + ' ' + x[2], int(x[3]))))
b2_2.collect()


# In[11]:


b2_join = b2_1.join(b2_2).map(lambda x: x[1]).distinct().groupByKey()\
            .map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[1])[:3]))
b2_join.collect()


# In[12]:


b2 = b2_join.map(lambda x: (month_to_number(x[0]), x)).sortByKey().map(lambda x: x[1])
b2.collect()


# ## 3. For each state, show the number of distinct players of each state, the results are sorted by the number of players

# In[13]:


b3 = transRDD.map(lambda x: (x[7], x[2])).distinct().groupByKey().mapValues(len).sortBy(lambda x: x[1])
b3.collect()


# ## 4. For each state, show the name of three oldest player in each state, the results are sorted by the state

# In[14]:


b4_1 = transRDD.map(lambda x: (x[2], x[7]))
b4_1.collect()


# In[15]:


b4_2= custRDD.map(lambda x: (x[0], (x[1] + ' ' + x[2], int(x[3]))))
b4_2.collect()


# In[16]:


b4 = b4_1.join(b4_2).map(lambda x: x[1]).distinct().groupByKey()\
            .map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:3]))
b4.collect()


# ## 5. For each state, show the average age of players in each state, the results are sorted by the average age

# In[17]:


b5_1 = transRDD.map(lambda x: (x[2], x[7]))
b5_1.collect()


# In[18]:


b5_2= custRDD.map(lambda x: (x[0], int(x[3])))
b5_2.collect()


# In[19]:


b5 = b5_1.join(b5_2).map(lambda x: (x[1][0], (x[1][1], 1))).distinct() \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).sortBy(lambda x: x[1])
b5.collect()


# ## 6. For each player ID, show the average number of game per month, the results are sorted by player ID

# In[20]:


b6_1 = transRDD.map(lambda x: (x[2], x[1].split('-')[0])).map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b:  a + b)\
                .map(lambda x: (x[0][0], (x[0][1], x[1])))
b6_1.collect()


# In[21]:


b6 = b6_1.map(lambda x: (x[0], (x[1][1], 1)))\
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
    .map(lambda x: (x[0], x[1][0] / x[1][1])).sortByKey()
b6.collect()


# ## 7. For each player ID, show the game with highest total cost, and the total cost of this game, the results are sorted by player ID

# In[22]:


b7 = transRDD.map(lambda x: (x[2], (x[4], float(x[3])))) \
        .reduceByKey(lambda x, y: (x[0], x[1] + y[1])) \
        .reduceByKey(lambda x, y: x if x[1] > y[1] else y).sortByKey()
b7.collect()


# ## 8. For each player ID, show the month with highest total cost, and the total cost in this month

# In[23]:


b8 = transRDD.map(lambda x: (int(x[2]), (x[1].split('-')[0], float(x[3]))))\
            .reduceByKey(lambda x, y: x if x[1] > y[1] else y)
b8.collect()


# ## 9. For each player ID, show the list of three games with most transactions, and the list of the number of transactions of these three games, the results are sorted by player IDs

# In[24]:


b9 = transRDD.map(lambda x: ((x[2], x[4]), float(x[3]))) \
            .reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0],(x[0][1],x[1])))\
            .groupByKey().mapValues(lambda games: sorted(games, key=lambda x: -x[1])[:3])
b9.collect()


# ## 10. For each game, show the number of transactions, the results are sorted by game

# In[25]:


b10 = transRDD.map(lambda x: (x[4], 1)).reduceByKey(lambda x, y: x + y) \
                .sortByKey()
b10.collect()


# ## 11. For each game, show the number of transactions, and the total cost, the results are sorted by game

# In[26]:


b11 = transRDD.map(lambda x: (x[4], (1, float(x[3])))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).sortByKey()
b11.collect()


# ## 12. For each month, show the number of transactions, and the total cost, the results are sorted by month

# In[27]:


b12 = transRDD.map(lambda x: (x[1].split('-')[0], (1, float(x[3])))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(lambda x: (month_to_number(x[0]), x)).sortByKey().map(lambda x: x[1])
b12.collect()

