#@@ json 数据导入

import json
path = 'E:/Job/MLanguage/Python/Workplace_DataAnalysis/pydata-book/datasets/bitly_usagov/example.txt'
records = [json.loads(line) for line in open(path)]
time_zones = [rec['tz'] for rec in records if 'tz' in rec]


#@@ defaultdict （对dict的功能扩展，当key不存在时，放回一个默认值，而不是KeyError）

###### 统计次数
from collections import defaultdict
def get_count(sequence):
  counts = defaultdict(int)
  for x in sequence:
    count[x] += 1
  return count
counts = get_count(time_zones)

###### 对字典进行排序
# 转化为list再排序
def top_counts(countDict, n = 10):
  countList = [(value, key) for key,value in countDict.items()]
  countList.sort()
  return countList[-n:]
top_counts(counts)

###### 利用库函数直接实现对字典的 统计+排序
from collection import Counter
counts = Counter(time_zones)
counts.most_common(10)

###### 利用pandas和numpy进行分析
from pandas import DataFrame, Series
import pandas as pd; import numpy as np
frame = DataFrame(records)    # records 是一个List，其中每一个元素是一个字典
frame['tz'][:10]    # 直接查看tz列的前10个数据，frame['tz']是一个Series对象
tz_counts = frame['tz'].value_counts()    # Series对象有一个value_counts方法用于统计出现的次数

clean_tz = frame['tz'].fillna('Missing')    # 填补的是 Nan
clean_tz[clean_tz == ''] = 'Unknown'    # 填补的是 空
tz_counts = clean_tz.value_counts()
tz_counts[:10].plot(kind='barh', rot=0)

# 把字符串的第一节分离出来
results = Series([x.split()[0] for x in frame.a.dropna()])

# 给出用户是否为windows系统的信息
cframe = frame[frame.a.notnull()]   # 将agent缺失项移除
operating_system = np.where(cframe['a'].str.contains('Windows'),'Windows','Not Windows') 
# 这里产生是一个numpy.ndarray类型的数组
by_tz_os = cframe.groupby(['tz', operating_system])
# groupby先对cframe的'tz'进行分组，再按照operating_system中的key进行分组
agg_counts = by_tz_os.size().unstack().fillna(0)
# size()对index进行计数，unstack()将二维的index变为一维index和一维的column，
