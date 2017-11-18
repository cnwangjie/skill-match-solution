package FD

import collection.mutable._
import util.control.Breaks._
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {

  private val conf = new SparkConf().setAppName("JN2")
  private val sc = new SparkContext(conf)
  private var data: RDD[Array[String]] = sc.emptyRDD

  private var attrNum: Int = 0

  private val dep: Map[String, Set[Int]] = Map()
  private val beDep: Map[Int, Set[String]] = Map()

  def combination(n: Int) = Range(0, n).flatMap(List.range(0, n).combinations(_))

  def compareDepRel(p: List[Int], r: Int): Boolean = {
    if (p.length == 0) {
      val fact = data.first()(r)
      return data.filter(_(r) != fact).count == 0
    }

    return data.map(item => (p.map(item(_)).mkString("|"), item(r)))
      .groupByKey()
      .values
      .filter(_.size > 1) // 留下可能有不同的
      .filter(i => !i.forall(_ == i.head)) // 留下有不同的
      .count == 0
  }

  def main(args: Array[String]) {
    println("===================================")
    println("         initialized")
    println("===================================")

    sc.setLogLevel("WARN")
    val inputFilePath = if (args.length > 0) args(0) else "file:///home/wangjie/Workspace/seu/dataset/test/bots_200_10.csv"
    val outputFilePath = if (args.length > 1) args(1) else "file:///home/wangjie/Workspace/seu/200_10.ans.byspark"
    val inputFile = sc.textFile(inputFilePath)
    val dataSize = inputFile.count
    println("===================================")
    println("         data loaded")
    println("===================================")
    println("data size: " + dataSize)
    data = inputFile.map(_.split(","))
    attrNum = data.first().length

    println("===================================")
    println("         data handled")
    println("===================================")
    println("num of attrbutions: " + attrNum)

    val startTime = DateTime.now.getMillis
    var lastLogTime: Long = 0
    var cycleTimes = 0
    val indexCombinations = combination(attrNum)
    val allCycleTimes = indexCombinations.length
    indexCombinations.foreach(r => {
      cycleTimes += 1
      val curTime = DateTime.now.getMillis
      if (curTime - lastLogTime > 1000) {
        println("progress: " + (cycleTimes.toDouble / allCycleTimes * 100).formatted("%.2f") +
        "% spend time: " + (DateTime.now.getMillis - startTime) / 1000 + "s")
        lastLogTime = curTime
      }
      for (i <- 0 to attrNum - 1) {

          if (!r.contains(i)) {
            var isMinimal = true

            if (beDep.contains(i)) {

              if (beDep(i).contains("")) isMinimal = false

              beDep(i).foreach(key => {
                if (isMinimal) {
                  val childSet = key.split(",").map(_.toInt)

                  var isChildSet = true
                  for (j <- 0 to childSet.length - 1) {

                    if (!r.contains(childSet(j))) {
                      isChildSet = false
                    }
                  }
                  if (isChildSet) isMinimal = false
                }
              })
            }

            if (isMinimal) {
              if (compareDepRel(r, i)) {
                val strR = r.mkString(",")
                if (dep.contains(strR)) {
                  dep(strR) += i
                } else {
                  dep(strR) = Set(i)
                }

                if (beDep.contains(i)) {
                  beDep(i) += strR
                } else {
                  beDep(i) = Set(strR)
                }
              }
            }
          }

      }
    })

    println("===================================")
    println("          calulated")
    println("===================================")
    println("time spent:" + (DateTime.now.getMillis - startTime) / 1000 + "s")
    println("result count: " + dep.size)

    val result: Set[String] = Set()
    dep.keys.foreach(key => {
      val ls = if (key == "") "" else key.split(",")
        .map(_.toInt)
        .map(_ + 1)
        .sortWith(_ < _)
        .map("column" + _)
        .mkString(",")
      val rs = dep(key)
        .map(_ + 1)
        .map("column" + _)
        .mkString(",")

      result += "[" + ls + "]:" + rs
    })
    sc.parallelize(result.toList).saveAsTextFile(outputFilePath)

    println("===================================")
    println("             done")
    println("===================================")
  }


}
