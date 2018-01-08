#######################################################
# 用shell操作hdfs
####################################################### 
#copyFromLocal    copyToLocal    
#get下载    put拷贝(put更宽松，可以把本地或者HDFS上的文件拷贝到HDFS中，而copyFromLocal只能上传本地文件)
hdfs dfs -ls /        # 查看hdfs的根目录
hdfs dfs -mkdir /test        # 根目录下常见文件夹
hdfs dfs -copyFromLocal /home/hadoop/mk.txt /test/        # 从本地上传
hafs dfs -cat /test/mk.txt  


#######################################################
# 用Python操作hdfs
####################################################### 

from hdfs3 import HDFileSystem

test_host = 'localhost'
test_port = 9000

if __name__ == '__main__':
