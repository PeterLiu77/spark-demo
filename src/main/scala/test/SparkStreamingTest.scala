package test

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest {

  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
        iter.flatMap{
          case (key,seq,opt) => Some(seq.sum + opt.getOrElse(0)).map(count => (key, count))
        }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountSum")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf,Seconds(5))
    sc.setCheckpointDir("/Users/liupeidong/Desktop")
    ssc.sparkContext.setLogLevel("WARN")

    val lines = ssc.socketTextStream("localhost", 7777)
    val tuple = lines.flatMap(_.split(" ")).map((_, 1))
    val result = tuple.updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),rememberPartitioner = true)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
