
# coding: utf-8

# In[1]:


rdd = sc.parallelize(range(100))


# In[2]:


n = rdd.count()


# In[3]:


summ = rdd.sum()


# In[4]:


mean = summ/n


# In[5]:


mean


# In[7]:


#median
sortedAndIndexed = rdd.sortBy(lambda x: x).zipWithIndex().map(lambda (value,key) : (key,value))
n = sortedAndIndexed.count()
if (n % 2==1):
    index = (n-1)/2
    print(sortedAndIndexed.lookup(index))
else:
    index1 = (n/2)-1
    index2 = n/2
    value1 = sortedAndIndexed.lookup(index1)[0]
    value2 = sortedAndIndexed.lookup(index2)[0]
    print((value1+value2)/2)


# In[14]:


#SD
from math import sqrt
sqrt(rdd.map(lambda x : pow(x-mean,2)).sum()/n)


# In[18]:


def summation(listt):
    s1 =0
    for i in listt:
        s1 +=i
    return s1


# In[27]:


l1 = [34,1,23,4,3,3,12,4,3,1]
n = len(l1)
print(n)


# In[26]:


summation(l1)


# In[28]:


mean = 88/n


# In[29]:


mean


# In[30]:


sqrt(l1.map(lambda x : pow(x-mean,2)).sum()/n)


# In[31]:


rdd2 = sc.parallelize([34,1,23,4,3,3,12,4,3,1])


# In[32]:


rdd2


# In[35]:


num = rdd2.count()


# In[36]:


summation= rdd2.sum()


# In[37]:


mean2 = summation/num


# In[41]:


sqrt(rdd2.map(lambda x : pow(x-mean2,2)).sum()/num)


# In[39]:


mean2


# In[42]:


rdd3 = sc.parallelize([34,1,23,4,3,3,12,4,3,1])


# In[46]:


numm = rdd3.count()
print(numm)
summa = rdd3.sum()
print(summa)
mean1 = summa/numm
print(mean1)


# In[44]:


mean1


# In[47]:


sortedAndIndexed = rdd3.sortBy(lambda x: x).zipWithIndex().map(lambda (value,key) : (key,value))
n = sortedAndIndexed.count()
if (n % 2==1):
    index = (n-1)/2
    print(sortedAndIndexed.lookup(index))
else:
    index1 = (n/2)-1
    index2 = n/2
    value1 = sortedAndIndexed.lookup(index1)[0]
    value2 = sortedAndIndexed.lookup(index2)[0]
    print((value1+value2)/2)


# In[48]:


#Skewness- How Asymmetric data is spread around the mean 


# In[62]:


rdd_skewness = sc.parallelize([34,1,23,4,3,3,12,4,3,1])


# In[63]:


sum = rdd_skewness.sum()
n = rdd_skewness.count()
mean = sum/n
print mean


# In[64]:


from math import sqrt
sd = sqrt(rdd_skewness.map(lambda x : pow(x-mean,2)).sum()/n)


# In[65]:


n = float(n)
skeness = n/((n-1)*(n-2))*rdd_skewness.map(lambda x : pow(x-mean,3)/pow(sd,3)).sum()
skeness


# In[61]:


#Kurtosis 
"""
Reports on the shape of the data
Indicates Outlier content within the data
Kurtosis is a measure of outlier content. The more outliers are present in examples of fuel consumption where engines didn't fail, the higher the false positive rate gets when choosing a threshold value which is too low.
"""
kurtosis = rdd_skewness.map(lambda x : pow(x-mean,4)/pow(sd,4)).sum()/1-n
kurtosis

