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

def hdfs_exists(hdfs_client):
  path = '/tmp/test'
  if hdfs_client.exists(path):
    hdfs_client.rm(path)
    
  hdfs_client.makedirs(path)
  
def hdfs_write_read(hdfs_client):
  data = b"hello hadoop" * 20   # b表示转化为二进制形式的
  file a = 'tmp/test/file_a'
  with hdfs_client.open(file_a, 'wb', replication=1) as f:
    f.write(data)
    
  with hdfs_client.open(file_a, 'rb') as f:
    out = f.read(len(data))   # read()里面的参数表示读取多少个字节
    
    assert out == data    # 断言，如果不相同，抛出错误

def hdfs_readlines(hdfs_client):
  file a = 'tmp/test/file_b'
  with hdfs_client.open(file_b, 'wb', replication=1) as f:
    f.write(b"hello\nhadoop")
    
  with hdfs_client.open(file_b, 'rb') as f:
    lines = f.readlines()   
    
  assert len(lines)==2
    
if __name__ == '__main__':
  hdfs_client = HDFilesSystem(host=test_host, port=test_port)
  
  hdfs_exists(hdfs_client)
  
  hdfs_write_read(hdfs_client)
  
  hdfs_readlines(hdfs_client)
  
  hdfs_client.disconnect()
  
  print("-" * 20)
  print("hello hadoop")
  
