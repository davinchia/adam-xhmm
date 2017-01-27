/**
  * Created by davinchia on 1/27/17.
  */
import org.apache.commons.math3.distribution.NormalDistribution
import org.scalameter._
import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val r = scala.util.Random
    val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
    val normDistDel : NormalDistribution = new NormalDistribution(-3, 1.0)
    val normDistDup : NormalDistribution = new NormalDistribution(3, 1.0)
    var obsB: ListBuffer[Double] = ListBuffer[Double]()

    for (i <- 1 to 200000) {
      var c = (r.nextDouble() * 10)

      if (r.nextDouble() < 0.5) c *= -1

      obsB += c
    }

    val obs = obsB.toList
    println("Observations generated.")

    var bigDec = Array.ofDim[BigDecimal](3, 200000)
    var double = Array.ofDim[Double](3, 200000)

    println("Start Big Decimal measurements..")
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
        //      emissions += (("Diploid", a) -> BigDecimal(normDistDip.density(cur)))
        //      emissions += (("Duplication", a) -> BigDecimal(normDistDup.density(cur)))
        //      emissions += (("Deletion", a) -> BigDecimal(normDistDel.density(cur)))
        bigDec(0)(a) = BigDecimal(normDistDel.density(cur))
        bigDec(1)(a) = BigDecimal(normDistDip.density(cur))
        bigDec(2)(a) = BigDecimal(normDistDup.density(cur))
      }
    }
    println("Big Decimal Total time: $time")

    println("Starting Double measurements..")
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
        //      emissions += (("Diploid", a) -> BigDecimal(normDistDip.density(cur)))
        //      emissions += (("Duplication", a) -> BigDecimal(normDistDup.density(cur)))
        //      emissions += (("Deletion", a) -> BigDecimal(normDistDel.density(cur)))
        double(0)(a) = normDistDel.density(cur)
        double(1)(a) = normDistDip.density(cur)
        double(2)(a) = normDistDup.density(cur)
      }
    }
    println("Big Decimal Total time: + $time")

//    val mem = config(
//      Key.exec.benchRuns -> 20
//    ) withMeasurer(new Measurer.MemoryFootprint) measure {
////      c_bigDec()
//    }
//    println(s"Total memory: $mem")







    def c_bigDec(): Unit = {
      for (a <- obs.indices) {
        val cur = obs(a)
        //      emissions += (("Diploid", a) -> BigDecimal(normDistDip.density(cur)))
        //      emissions += (("Duplication", a) -> BigDecimal(normDistDup.density(cur)))
        //      emissions += (("Deletion", a) -> BigDecimal(normDistDel.density(cur)))
        bigDec(0)(a) = BigDecimal(normDistDel.density(cur))
        bigDec(1)(a) = BigDecimal(normDistDip.density(cur))
        bigDec(2)(a) = BigDecimal(normDistDup.density(cur))
      }
    }
  }
}
