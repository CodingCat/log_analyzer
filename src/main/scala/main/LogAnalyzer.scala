package main

import org.apache.spark.SparkContext

object LogAnalyzer {

  def initRangeArray(): (Array[Int], Array[(Double, Int)]) = {
    val arr = new Array[Int](500)
    for (i <- 0 until 500) {
      arr(i) = 20 * (i + 1)
    }

    val responseTimeArray = new Array[(Double, Int)](500)
    for (i <- 0 until 500) {
      responseTimeArray(i) = 0.0 -> 0
    }
    (arr, responseTimeArray)
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkContext._
    val sc = new SparkContext()
    val (rangeArray, responseTimeArray) = initRangeArray()
    val filteredLines = sc.textFile(args(0)).filter(_.contains("video"))
    val rangeToCount = filteredLines.map(line => {
      val Array(videoId, demand, submitTime, startTime, endTime) = line.split(' ')
      var i = 0
      while (rangeArray(i) < demand.toInt) i += 1
      (rangeArray(i), (startTime.toLong - submitTime.toLong, endTime.toLong - startTime.toLong))
    }).mapValues { case (waitingTime, responseTime) => ((waitingTime, responseTime), 1)}.
      reduceByKey { case (((waitingTime1, responseTime1), cnt1), ((waitingTime2, responseTime2), cnt2)) =>
      ((waitingTime1 + waitingTime2, responseTime1 + responseTime2), cnt1 + cnt2)
    }.
      map { case (range, ((waitingTimeSum, responseTimeSum), count)) =>
      (range, (waitingTimeSum / count, responseTimeSum / count))
    }
    val driverSideRangeCount = rangeToCount.collect()
    for ((range, (waitingTimeAvr, responseTimeAvr)) <- driverSideRangeCount) {
      println(range, (waitingTimeAvr, responseTimeAvr, waitingTimeAvr + responseTimeAvr))
    }
  }

}
