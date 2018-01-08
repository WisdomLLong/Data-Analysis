'''
问题：用Python编写简单的MapReduce程序
注意：
1、Python编程中，groupby和itemgetter函数的用法
2、hdfs的shell命令用法
'''

#######################################################
# Map (hdfs_map.py)
#######################################################
import sys

def read_input(file):
  for line in file:
    yield line.split()
    
def main():
  data = read_input(sys.stdin)
  
  for words in data:
    for word in words:
      print('%s%s%d' % (word, '\t', 1))
      
if __name__=='__main__'：
  main()

  
#######################################################
# Reduce  (hdfs_reduce.py)
#######################################################
import sys
from operator import itemgetter
from itertools import groupby

def read_mapper_output(file, separator='\t'):
  for line in file:
    yield line.rstrip().split(separator, 1)
    # restrip() 意思是remove字符串尾部的空格
    # split('\t', 1)  意思是以'\t'作为分隔符，只拆一次字符串
    
def main():
  data = read_mapper_output(sys.stdin)
  
  for current_word, group in groupby(data, itemgetter(0)):
    total_count = sum(int(count) for current_word, count in group)
    # itemgetter(0) 表示取出data中每一个元素的第一个成分
    # groupby() 是对已有的数据进行分组，按照itemgetter(0)进行分组
    #@@ groupby 像 uniq 函数一样，只能将连续的数值相同的元素分在一组，所以data应该是已经排序了的数据
    
    print('%s%s%d' % (current_word, '\t', total_count))
    
if __name__ == '__main__':
  main()
  

#######################################################
# 利用shell简单测试
####################################################### 

echo "a b c d a b c" | /opt/anaconda2/bin/python hdfs_map.py
# 查看读取后的数据

echo "a b c d a b c" | /opt/anaconda2/bin/python hdfs_map.py | sort -k1,1 | /opt/anaconda2/bin/python hdfs_reduce.py



#######################################################
# 利用Hadoop的MapReduce进行测试
####################################################### 
# 利用 jps 查看已经开启的服务
DataNode  ResourceManager SecondaryNameNode NameNode  NodeManager

# 输入命令运行程序
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/
hadoop-streaming-2.7.2.jar -files "hdfs_map.py, hdfs_reduce.py" -input /test/
mk.txt -output /tmp/wordcounttest -mapper "/opt/anaconda2/bin/python hdfs_map.py"
-reducer "/opt/anaconda2/bin/python hdfs_reduce.py"

# 查看输出结果
hdfs dfs -ls /tmp/wordcounttest
hdfs dfs -cat part-00000

    
