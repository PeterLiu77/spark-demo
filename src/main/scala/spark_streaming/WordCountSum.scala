package spark_streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object WordCountSum {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountSum")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setCheckpointDir("/Users/liupeidong/Desktop")
    ssc.sparkContext.setLogLevel("WARN")

    // 状态更新函数
    val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
      // flatMap可以过滤掉Option里的null的数据
      iter.flatMap{
        case (key,seq,opt) => Some(seq.sum + opt.getOrElse(0)).map(count => (key, count))
      }
//      iter.flatMap(x => {
//        Some(x._2.sum + x._3.getOrElse(0)).map((x._1,_))
//      })
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
