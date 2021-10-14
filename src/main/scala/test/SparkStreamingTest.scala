package test

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountSum")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setCheckpointDir("/Users/liupeidong/Desktop")
    ssc.sparkContext.setLogLevel("WARN")

    // 状态更新函数
    val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
      iter.flatMap{
        case (key,seq,opt) => Some(seq.sum + opt.getOrElse(0)).map(count => (key, count))
      }
    }

    // 业务代码 nc -lk 7777
    val lines = ssc.socketTextStream("localhost", 7777)
    val tuple = lines.flatMap(_.split(" ")).map((_, 1))
    val result = tuple.updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),rememberPartitioner = true)
    result.print()

    // 执行
    ssc.start()
    ssc.awaitTermination()
  }
}
