package com.peach

import com.databricks.spark.avro._
import com.peach.arvo.CustomerAddress
import org.apache.avro.io.Encoder
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过spark-avro包读取将avro数据解析DataFrame
 * @author peach
 * 2017-03-02
 */
object App {
  def main(args: Array[String]): Unit = {
    val path = "F:\\data\\customeraddress.avro"

    val conf = new SparkConf().setAppName("spark-avro-test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = sqlContext.read.avro(path).filter("ca_address_sk > 999000").filter("ca_country = 'United States'")
    df.show()
    df.registerTempTable("customer_address")
    val df1 = sqlContext.sql("select * from customer_address limit 5")
    df1.show()

//    df1.as[CustomerAddress](Encoders.bean[CustomerAddressData])
//    df.write.avro("F://tmp//avro") //将结果输出到目录
  }



  case class CustomerAddressData(ca_address_sk: BigInt,ca_address_id: String,ca_street_number: String,ca_street_name: String,ca_street_type: String,ca_suite_number: String,ca_city: String,ca_county: String,ca_state: String,ca_zip: String,ca_country: String,ca_gmt_offset: Double,ca_location_type: String)
}
