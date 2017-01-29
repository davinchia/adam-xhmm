/**
  * Created by davinchia on 1/27/17.
  */

import breeze.linalg.DenseMatrix
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object DistXHMM {
  class Sample(val observations: Array[Double], val len: Int) extends Serializable {
    var emissions   : Array[Array[Double]] = Array[Array[Double]]()
    var forward    : Array[Array[Double]] = Array[Array[Double]]()
    var backward   : Array[Array[Double]] = Array[Array[Double]]()
    var viterbiPath: Array[Int]           = Array[Int]()

    def obs     = observations

    def emiss   = emissions
    def fwd     = forward
    def bck     = backward
    def vitPath = viterbiPath

    override def toString: String =
      "(observations: " + obs.mkString(", ") + " emission: " + emiss.deep.mkString(", ") +
      " forward: " + fwd.deep.mkString(", ") + " backward: " + bck.deep.mkString(", ") +
      "Viterbi Path: " + vitPath.mkString(", ")+ ")"
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)

    val broadTrans = sc.broadcast(new DenseMatrix(3, 3, Array(1.0,1.0,1.0,2.0,2.0,2.0,3.0,3.0,3.0)))

    var fakeSamples = new mutable.ListBuffer[Sample]
    var obsArray = (1 to 200000).toArray.map(_.toDouble)
    for (i <- 1 to 200) {
      fakeSamples += new Sample(obsArray, obsArray.length)
    }

//    val samples : RDD[Sample] = sc.parallelize(Array(new Sample(Array(1.0, 2.0, 3.0), 3),
//                                                     new Sample(Array(4.0, 5.0, 6.0), 3),
//                                                     new Sample(Array(7.0, 8.0, 9.0), 3)))

    val samples: RDD[Sample] = sc.parallelize(fakeSamples.toArray[Sample])

    def add(iRow: IndexedRow): Double = {
      iRow.vector(0) + broadTrans.value(0, 0)
    }

    def calc_emission(ob: Double): Array[Double] = {
      val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
      val normDistDel : NormalDistribution = new NormalDistribution(-3, 1.0)
      val normDistDup : NormalDistribution = new NormalDistribution(3, 1.0)

      Array(normDistDel.density(ob), normDistDip.density(ob), normDistDup.density(ob))
    }

    def calc_forward(len: Int, emissions: Array[Array[Double]]): Array[Array[Double]] = {
      val transitions = broadTrans.value
      var fwdCache = Array.ofDim[Double](3, len)
      val states = (0 to 2)

      fwdCache(0)(0) = 0.01; fwdCache(0)(1) = 0.98; fwdCache(0)(2) = 0.01

      for (i <- 1 until len) {
        for (s <- states) {
          fwdCache(s)(i) = (states map { e =>
            fwdCache(e)(i-1) * transitions(e,s)
          } sum) * emissions(i)(s)
        }
      }

      fwdCache
    }

    var emiss_samples = samples.map( e => {
      e.emissions = e.observations map ( a => calc_emission(a) )
      e.forward  = calc_forward(e.observations.length, e.emissions)
      e.backward = calc_forward(e.observations.length, e.emissions)
    })

    val t0 = System.nanoTime()
    emiss_samples.collect().foreach(println)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + " seconds")
  }
}
