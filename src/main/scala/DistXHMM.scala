/**
  * Created by davinchia on 1/27/17.
  */

import breeze.linalg.DenseMatrix
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object DistXHMM {
  def calc_emission(ob: Double): Array[BigDecimal] = {
    val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
    val normDistDel : NormalDistribution = new NormalDistribution(-3, 1.0)
    val normDistDup : NormalDistribution = new NormalDistribution(3, 1.0)

    Array(BigDecimal(normDistDel.density(ob)), BigDecimal(normDistDip.density(ob)), BigDecimal(normDistDup.density(ob)))
  }

  def calc_forward(len: Int, emissions: Array[Array[BigDecimal]], transitions: Array[Array[Array[BigDecimal]]]): Array[Array[BigDecimal]] = {
    var fwdCache = Array.ofDim[BigDecimal](3, len)
    val states = (0 to 2)

    fwdCache(0)(0) = 0.01 * emissions(0)(0)
    fwdCache(1)(0) = 0.98 * emissions(0)(1)
    fwdCache(2)(0) = 0.01 * emissions(0)(2)

    for (i <- 1 until len) {
      for (s <- states) {
        fwdCache(s)(i) = (states map { e =>
          fwdCache(e)(i-1) * transitions(i)(e)(s)
        } sum) * emissions(i)(s)
      }
    }

    fwdCache
  }

  def calc_backward(len: Int, emissions: Array[Array[BigDecimal]], transitions: Array[Array[Array[BigDecimal]]]): Array[Array[BigDecimal]] = {
    var bckCache = Array.ofDim[BigDecimal](3, len+1)
    val states = (0 to 2)

    bckCache(0)(len) = 1
    bckCache(1)(len) = 1
    bckCache(2)(len) = 1

    for (i <- (1 to len-1).reverse) {
      for (s <- states) {
        bckCache(s)(i) = states map { e =>
          transitions(i)(s)(e) * emissions(i)(e) * bckCache(e)(i+1)
        } sum
      }
    }

    bckCache
  }

  def calc_viterbi(fwdCache: Array[Array[BigDecimal]], bckCache: Array[Array[BigDecimal]]): Array[Int] = {
    var path : mutable.ListBuffer[Int] = new mutable.ListBuffer[Int]
    val states = (0 to 2)
    for (t <- 0 until fwdCache(0).length) {
      path += (states map { (s) => (fwdCache(s)(t) * bckCache(s)(t+1), s)
      } maxBy (_._1))._2
    }
    path.toArray[Int]
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
//      .set("spark.executor.memory", "5g")
//      .set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)

    val broadTrans = sc.broadcast(new DenseMatrix(3, 3, Array(1.0,1.0,1.0,2.0,2.0,2.0,3.0,3.0,3.0)))

    var fakeSamples = new mutable.ListBuffer[Sample]
    var obsArray = (1 to 200000).toArray.map(_.toDouble)
    for (i <- 1 to 2000) {
      fakeSamples += new Sample(obsArray, obsArray.length)
    }

    println("Start parallelising..")
    var samples: RDD[Sample] = sc.parallelize(fakeSamples.toArray[Sample])
    println("Done parallelising.")

    println("Start calculations..")
    val t0 = System.nanoTime()
//    samples = samples.map( e => {
//      e.emissions   = e.observations map ( a => calc_emission(a) )
//      e.forward     = calc_forward(e.observations.length, e.emissions, broadTrans.value)
//      e.backward    = calc_backward(e.observations.length, e.emissions, broadTrans.value)
//      e.viterbiPath = calc_viterbi(e.fwd, e.bck)
//      e
//    })

    val viterbis = samples.map(e => {e.viterbiPath}).collect()
    val t1 = System.nanoTime()

    viterbis.foreach( e => println(e.deep.mkString(", ")))
    println("Elapsed time: " + (t1 - t0)/1000000000 + " seconds")
    println("Done calculations.")
  }
}
