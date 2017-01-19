/**
  * Created by davinchia on 1/13/17.
  */

import scala.collection.Map
import scala.collection.mutable.ListBuffer
import breeze.linalg.DenseMatrix
import org.apache.commons.math3.distribution.{NormalDistribution}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

object SVD {
  def main(args: Array[String]) {

    // Spark context
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("spark://davins-air.middlebury.edu:7077")
//      .setMaster("local[2]")
      .set("spark.executor.memory", "5g")
      .set("spark.driver.memory", "5g")
    val sc = new SparkContext(conf)

    println("=== Reading file.. ")

    // Input
    val arr = sc.textFile("file:///Users/davinchia/Desktop/Projects/Scripts/Random/matrix.txt").flatMap(_.split(",").map(_.toDouble))

    println("=== Finished reading file")

    // To be clear, the DenseMatrix we use belongs to Breeze and not MLlib
    // Breeze allows us to easily do matrix addition with transposes
    // All DenseMatrices in this file refers to Breeze DenseMatrices; will be indicated otherwise if they refer to MLLib

    // DenseMatrix reads in the matrix in column-major order; our test matrix is being transposed
    // Acceptable as it is likely our actual input is also transposed
    println("=== Turning RDD into DenseMatrix")
    val rt = new DenseMatrix(265, 30, arr.take(7950))
    println("=== Finished turning RDD into DenseMatrix")
    val r = rt.t

    // Turn matrix into RDD to allow us to utilise MLlib SVD on it
    println("=== Creating RowMatrix")
    val rm: RowMatrix = new RowMatrix(matrixToRDD(rt, sc)) // We are feeding in a transpose matrix, so we only need V
    val numSamples = 29 // n - 1
    println("=== RowMatrix created ")

    var t0 = System.nanoTime()
    println("=== Start SVD")
    // we do not need u, since SVD is done on the transpose
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rm.computeSVD(Math.min(rm.numRows(), rm.numCols()).toInt, computeU = false)
    println("=== Done SVD")
    var t1 = System.nanoTime()
    println("=== Elapsed time: " + (t1 - t0)/1000000000.0 + "seconds")

    // SVD is done in a distributed manner. All operations are done locally, from here onwards.
    // Do we want to make this distributed?
    val d: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
    val dmV : DenseMatrix[Double] = new DenseMatrix(V.numCols, V.numRows, V.toArray) // Transform V to DenseMatrix

    val vals = d.toArray.map( x => x * x / numSamples ) // Transform singular values into eigenvalues

    // Calculate total, relative values, and eigenvectors to remove
    val total = vals.sum
    val relVar = vals.map( x => x / total )
    val toRemove = relVar.map( x => x >= 0.7 / numSamples)

    // Equation 1
    var normR = r
    for ( a <- 0 to toRemove.length-1 ) {
      if (toRemove(a)) {
        // c * ct * R, where R is the original, samples by targets, matrix
        val c = dmV(::, a); val ct = c.t;
        val res = c * ct * r
        normR -= res
      }
    }

    // Turn each row in normR into a z-score
    var normZ = normR
    for ( a <- 0 to normR.rows-1 ) {
      val row = normZ(a, ::); val cols = normZ.cols

      var total = 0.0
      for ( b <- 0 to cols-1 ) {
        total += row(b)
      }
      val mean = total / cols

      var std = 0.0
      for ( b <- 0 to cols-1 ) {
        std += Math.pow((row(b) - mean), 2)
      }
      std = Math.sqrt(std / (cols-1))

      for ( b <- 0 to cols-1 ) {
        normZ(a,b) = (normZ(a,b) - mean) / std
      }
    }

    println(normZ)

    // These are preset constant used to calculate probabilities
    val p = 0.00000001
    val t = 6
    val q = 1 / t
    val m = 3
    val D = 70000

    val states = List("Diploid", "Duplication", "Deletion")
    val start_probability : String => Double = {
      case "Diploid"      => 1.0
      case "Duplication"  => 0.0
      case "Deletion"     => 0.0
    }

    // Table 1
    val transition_probability_2 : (String, String) => Double = {
      case ("Diploid", "Diploid")     => 1 - 2*p
      case ("Diploid", "Duplication") => p
      case ("Diploid", "Deletion")    => p

      case ("Duplication", "Duplication") => 1 - q
      case ("Duplication", "Diploid")     => q
      case ("Duplication", "Deletion")    => 0.0

      case ("Deletion", "Deletion")    => 1 - q
      case ("Deletion", "Diploid")     => q
      case ("Deletion", "Duplication") => 0.0
    }

    val normDistDip = new NormalDistribution(0.0, 1.0)
    val normDistDel = new NormalDistribution(-m, 1.0)
    val normDistDup = new NormalDistribution(m, 1.0)

    // Generate obs for each row
    for ( r <- 0 to normZ.rows-1 ) {
      Viterbi.seenProbMap = Map()
      val listBuffer = new ListBuffer[Double]()
      for ( a <- 0 to normZ.cols-1 ) {
        listBuffer += normZ(r, a)
      }
      val obs : List[Double] = listBuffer.toList // Row from Z matrix

      // Map: (State, Time) => Probability
      var emission_probability : Map[(String, Int), Double] = Map()
      // Generate emission probabilities for each sample
      for ( a <- 0 to obs.length-1 ) {
        val cur = obs(a)
        emission_probability += ( ("Diploid", a) -> normDistDip.density(cur) )
        emission_probability += ( ("Duplication", a) -> normDistDup.density(cur) )
        emission_probability += ( ("Deletion", a) -> normDistDel.density(cur) )
      }

      if ( r == 18 ) {
        println("=== Running Viterbi..")
        t0 = System.nanoTime()
        println(Viterbi.viterbi(obs, states, start_probability, transition_probability_1, emission_probability))
        t1 = System.nanoTime()
        println("=== Done Viterbi")
        println("=== Elapsed time: " + (t1 - t0)/1000000000.0 + "seconds")
      }
    }

  }

  def matrixToRDD(m: DenseMatrix[Double], sc: SparkContext): RDD[Vector] = {
    val columns = m.toArray.grouped(m.rows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }
}

//    println("=== RowMatrix: ")
//    println(rm.numRows())
//    println(rm.numCols())
//    println(rm.rows.collect().foreach(println))

//    println(r.rows)
//    println(r.cols)
//    println(r)

//    println("=== DenseMatrix: ")
//    println(arr) // Note this print statement will not work since arr is RDD and on separate machines
//    println(dm)
//    println(dm.numRows)

//    println("U: ")
//    println(U.numRows())
//    println(U.numCols())
//    println(U.rows.collect().foreach(println))

//    println("d: ")
//    println(d)
//    println(d.size)

//    println("=== V: ")
//    println(V)
//    V.toArray.foreach(println)
//    println(V.numRows)
//    println(V.numCols)
