1.工程添加avro的maven依赖
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>${avro.servion}</version>
</dependency>

2.首先定义一个schema文件CustomerAddress.avsc，内容如下
{
  "namespace":"com.peach.arvo",  #生成对应的java包路径
  "type": "record",
  "name": "CustomerAddress",
  "fields": [
    {"name":"ca_address_sk","type":"long"},
    {"name":"ca_address_id","type":"string"},
    {"name":"ca_street_number","type":"string"},
    {"name":"ca_street_name","type":"string"},
    {"name":"ca_street_type","type":"string"},
    {"name":"ca_suite_number","type":"string"},
    {"name":"ca_city","type":"string"},
    {"name":"ca_county","type":"string"},
    {"name":"ca_state","type":"string"},
    {"name":"ca_zip","type":"string"},
    {"name":"ca_country","type":"string"},
    {"name":"ca_gmt_offset","type":"double"},
    {"name":"ca_location_type","type":"string"}
  ]
}

3.在官网下载http://apache.communilink.net/avro/avro-1.8.1/java/avro-tools-1.8.1.jar

4.运行命令生成对应的java文件，在当前目录下生成对应schema的CustomerAddress.java类
java -jar avro-tools-1.8.1.jar complie schema CustomerAddress.avsc .

5.创建对应的包，将java文件拷贝至工程对应包下

6.事例数据resources/customer_address.dat

