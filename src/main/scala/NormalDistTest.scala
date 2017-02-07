/**
  * Created by davinchia on 1/27/17.
  */
import org.apache.commons.math3.distribution.NormalDistribution
import org.scalameter._
import scala.collection.mutable.ListBuffer
import breeze.stats.distributions._

object NormalDistTest {
  def main(args: Array[String]): Unit = {
    val r = scala.util.Random
    val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
    val normDistDel : NormalDistribution = new NormalDistribution(-3, 1.0)
    val normDistDup : NormalDistribution = new NormalDistribution(3, 1.0)
    val c = Math.log10(Math.sqrt(2 * Math.PI))
    val b = Math.log10(Math.E)

    var obsB: ListBuffer[Double] = ListBuffer[Double]()

    for (i <- 1 to 200000) {
      var c = (r.nextDouble() * 10)

      if (r.nextDouble() < 0.5) c *= -1

      obsB += c
    }

    val obs = obsB.toArray
    println("Observations generated.")

    var breezeNorm = Gaussian(3.0, 1.0)
    var bigDec = Array.ofDim[BigDecimal](3, 200000)
    var double = Array.ofDim[Double](3, 200000)

    println("Start Apache measurements..")
    var time = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> true
    ) withWarmer {
       new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      for (a <- obs.indices) {
        val cur = obs(a)
        double(0)(a) = normDistDel.density(cur)
        double(1)(a) = normDistDip.density(cur)
        double(2)(a) = normDistDup.density(cur)
      }
    }
    println(s"Apache time: $time")

    println("Starting Approx measurements..")
    time = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> true
    )  withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      for (a <- obs.indices) {
        val cur = obs(a); var frac = c - b * (cur-3) *(cur-3)/2
        double(0)(a) = (-frac)
        double(1)(a) = (-frac)
        double(2)(a) = (-frac)
      }
    }
    println(s"Approx time: $time")

    println("Start Breeze measurements..")
    time = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> true
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      for (a <- obs.indices) {
        val cur = obs(a)
        double(0)(a) = (breezeNorm.pdf(cur))
        double(1)(a) = (breezeNorm.pdf(cur))
        double(2)(a) = (breezeNorm.pdf(cur))
      }
    }
    println(s"Breeze time: $time")

//    var mem = config(
//      Key.exec.benchRuns -> 20
//    ) withMeasurer(new Measurer.MemoryFootprint) measure {
//      for (a <- obs.indices) {
//        val cur = obs(a)
//        bigDec(0)(a) = BigDecimal(normDistDel.density(cur))
//        bigDec(1)(a) = BigDecimal(normDistDip.density(cur))
//        bigDec(2)(a) = BigDecimal(normDistDup.density(cur))
//      }
//    }
//    println(s"Apache memory: $mem")
//
//    mem = config(
//      Key.exec.benchRuns -> 20
//    ) withMeasurer(new Measurer.MemoryFootprint) measure {
//      for (a <- obs.indices) {
//        val cur = obs(a); var frac = c - b * (cur-3) *(cur-3)/2
//        bigDec(0)(a) = BigDecimal(-frac)
//        bigDec(1)(a) = BigDecimal(-frac)
//        bigDec(2)(a) = BigDecimal(-frac)
//      }
//    }
//    println(s"Apache memory: $mem")
//
//    mem = config(
//      Key.exec.benchRuns -> 20
//    ) withMeasurer(new Measurer.MemoryFootprint) measure {
//      for (a <- obs.indices) {
//        val cur = obs(a)
//        bigDec(0)(a) = BigDecimal(breezeNorm.pdf(cur))
//        bigDec(1)(a) = BigDecimal(breezeNorm.pdf(cur))
//        bigDec(2)(a) = BigDecimal(breezeNorm.pdf(cur))
//      }
//    }
//    println(s"Apache memory: $mem")



  }
}
