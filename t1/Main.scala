package AR

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object Main {

  private val conf = new SparkConf().setAppName("AR")
    .set("spark.blacklist.enabled", "true")
    .set("spark.blacklist.timeout", "60s")
    .set("spark.cores.max", "168")
    .set("spark.driver.cores", "23")
    .set("spark.driver.maxResultSize", "2G")
    .set("spark.executor.cores", "23")
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    .set("spark.network.timeout", "300s")
    .set("spark.speculation", "true")
  private val sc = new SparkContext(conf)

  def findItem(u: Set[Int], r: Array[Tuple2[Int, Set[Int]]]): Int = {
    var i = 0
    while (i < r.size) {
      if (!u.contains(r(i)._1) && r(i)._2.subsetOf(u)) return r(i)._1
      i += 1
    }
    return 0
  }

  def main(args: Array[String]) {
    println("===================================")
    println("         initialized")
    println("===================================")

    sc.setLogLevel("ERROR")
    val inputFilePath = if (args.length > 0) args(0) else "hdfs:///user/cluster/data/"
    val outputFilePath = if (args.length > 1) args(1) else "hdfs:///user/cluster/t1.ans/"
    val tempFilePath = if (args.length > 2) args(2) else ""

    println("loading D.dat...")
    val d = sc.textFile((inputFilePath + "/D.dat"), 2000)
      .filter(_ != "")
      .map(_.split(" ")
      .filter(_ != "").map(_.toInt))
      .persist(MEMORY_AND_DISK)

    val dSize = d.count
    println("===================================")
    println("         D.dat loaded")
    println("===================================")
    println("size of D.dat: " + dSize)

    println("creating model...")
    val model = new FPGrowth()
      .setMinSupport(0.092)
      .setNumPartitions(2000)
      .run(d)

    println("====================================")
    println("stage: model created")
    println("====================================")

    println("calculating freq item sets...")
    val fiss = model.freqItemsets.persist(MEMORY_AND_DISK)
    val fissSize = fiss.count
    println("====================================")
    println("    fiss calculated")
    println("====================================")
    println("size of fiss: " + fissSize)

    val freq_model_path = outputFilePath + "/freq_model"
    fiss.sortBy(_.freq)
      .map(i => i.items.mkString("{", ",", "}") + ": " + i.freq)
      .saveAsTextFile(freq_model_path)
    println("====================================")
    println("freq model saved at path: " + freq_model_path)
    println("====================================")

    println("generating association rules...")
    val rules = model.generateAssociationRules(0).persist(MEMORY_AND_DISK)
    val rulesSize = rules.count
    println("====================================")
    println("    rules calculated")
    println("====================================")
    println("size of rules: " + rulesSize)

    val u = sc.textFile(inputFilePath + "/U.dat", 5000)
      .map(_.split(" ").map(_.toInt))

    val handledRules = rules
      .sortBy(_.consequent(0))
      .sortBy(-_.confidence)
      .map(i => (i.consequent(0), i.antecedent.toSet))
    val handledRulesBC = sc.broadcast(handledRules.collect)

    println("recommanding...")
    val result = u.map(_.toSet)
      .map(findItem(_, handledRulesBC.value))
      .persist(MEMORY_AND_DISK)
    println("====================================")
    println("    recommand completed")
    println("====================================")

    val recommand_path = outputFilePath + "/recommand_result"
    result.coalesce(100, false).saveAsTextFile(recommand_path)
    println("====================================")
    println("recommand result saved at path: " + recommand_path)
    println("====================================")

    println("====================================")
    println("               done")
    println("====================================")
  }

}
