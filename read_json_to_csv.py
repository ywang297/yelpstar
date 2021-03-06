# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 02:59:58 2018

@author: yishu
"""

import pandas as pd
import numpy as np

business = pd.read_json('yelp_academic_dataset_business.json', lines=True)
business_TOR = business[business['city']=='Toronto']
business_TOR.to_csv('business_TOR.csv', index=False, sep=',')

checkin = pd.read_json('yelp_academic_dataset_checkin.json', lines=True)
checkin.to_csv('checkin.csv', index=False, sep=',')

tip = pd.read_json('yelp_academic_dataset_tip.json', lines=True)
tip.to_csv('tip.csv', index=False, sep=',')

##lead to computer failure review = pd.read_json('yelp_academic_dataset_review.json', lines=True)
f = open('yelp_academic_dataset_review.json', encoding='utf8')
data = f.readlines()


line_t = [line.rstrip('\n') for line in data[:600000]]

#df_t = pd.read_json(data_t, lines=True)
#not working

line_t_test = line_t[:20]
data_test = ''.join(line_t_test)

import ijson
objects = ijson.items(data_test, '')
#for obj in objects:
#    print(obj['review_id'])
#not working


import regex as re    
line = line_t_test[0]    
dict={}
dict['review_id'] = re.search('\"review_id\":\".*\",\"user_id\":\"', line).group()[13:-13]
dict['user_id'] = re.search('\"user_id\":\".*\",\"business_id\":\"', line).group()[11:-17] 
dict['business_id'] = re.search('\"business_id\":\".*\",\"stars\":', line).group()[15:-10]
dict['stars'] = re.search('\"stars\":[0-9],\"date\":\"', line).group()[8]
dict['date'] = re.search('\"date\":\".*\",\"text\":\"', line).group()[8:-10]  
dict['text'] = re.search('\"text\":\".*\",\"useful\":', line).group()[8:-11]
dict['useful'] = re.search('\"useful\":[0-9],\"funny\":', line).group()[9] 
dict['funny'] = re.search('\"funny\":[0-9],\"cool\":', line).group()[8] 
dict['cool'] = re.search('\"cool\":[0-9]}', line).group()[7] 

import time  
from tqdm import tqdm


line_t = [line.rstrip('\n') for line in data[5400000:]]  
t0=time.time()
dicts_list=[]
for line in tqdm(line_t): 
    dict={}
    if re.search('\"review_id\":\".*\",\"user_id\":\"', line):
        dict['review_id'] = re.search('\"review_id\":\".*\",\"user_id\":\"', line).group()[13:-13]
    if re.search('\"user_id\":\".*\",\"business_id\":\"', line):
        dict['user_id'] = re.search('\"user_id\":\".*\",\"business_id\":\"', line).group()[11:-17] 
    if re.search('\"business_id\":\".*\",\"stars\":', line):    
        dict['business_id'] = re.search('\"business_id\":\".*\",\"stars\":', line).group()[15:-10]
    if re.search('\"stars\":[0-9],\"date\":\"', line):    
        dict['stars'] = re.search('\"stars\":[0-9],\"date\":\"', line).group()[8]
    if re.search('\"date\":\".*\",\"text\":\"', line):    
        dict['date'] = re.search('\"date\":\".*\",\"text\":\"', line).group()[8:-10] 
    if re.search('\"text\":\".*\",\"useful\":', line):    
        dict['text'] = re.search('\"text\":\".*\",\"useful\":', line).group()[8:-11]
    if re.search('\"useful\":[0-9],\"funny\":', line):    
        dict['useful'] = re.search('\"useful\":[0-9],\"funny\":', line).group()[9] 
    if re.search('\"funny\":[0-9],\"cool\":', line):    
        dict['funny'] = re.search('\"funny\":[0-9],\"cool\":', line).group()[8]
    if re.search('\"cool\":[0-9]}', line):    
        dict['cool'] = re.search('\"cool\":[0-9]}', line).group()[7] 
    dicts_list.append(dict)
t1=time.time()
print('time:', t1-t0) 
#time: 33.69107627868652
t0=time.time()
review_0 = pd.DataFrame(dicts_list)
t1=time.time()
print('time:', t1-t0) 
#time: 0.8147826194763184  
## make 4 columns numeric
review_0['stars'] = pd.to_numeric(review_0['stars'])
review_0['useful'] = pd.to_numeric(review_0['useful'])
review_0['funny'] = pd.to_numeric(review_0['funny'])
review_0['cool'] = pd.to_numeric(review_0['cool'])
## reorder columns
cols=['review_id', 'user_id', 'business_id', 'stars', 'date', 'text', 'useful', 'funny', 'cool']
review_0 = review_0[cols]
review_0.to_csv("\yishu\Documents\insight\review_9.csv", index=False, sep=',')


