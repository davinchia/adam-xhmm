/**
  * Created by davinchia on 1/13/17.
  */

import scala.collection.Map
import scala.collection.mutable.ListBuffer
import breeze.linalg.DenseMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import Model._
import Viterbi._

object SVD {
  def main(args: Array[String]) {

    /**
      * Spark is configured to run using Standalone Cluster mode.
      */

    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("spark://davins-air.middlebury.edu:7077")
//      .setMaster("local[2]")
      .set("spark.executor.memory", "5g")
      .set("spark.driver.memory", "5g")
    val sc = new SparkContext(conf)

    println("Reading file.. ")

    // Input
    val arr = sc.textFile("file:///Users/davinchia/Desktop/Projects/Scripts/Random/matrix.txt").flatMap(_.split(",").map(_.toDouble))

    println("Finished reading file")

    /**
      * To be clear, the DenseMatrix we use belongs to Breeze and not MLlib.
      * Breeze allows us to easily do matrix addition with transposes.
      * All DenseMatrices in this file refers to Breeze DenseMatrices; will be indicated otherwise if they refer to MLLib.
      * DenseMatrix reads in the matrix in column-major order; our test matrix is being transposed.
      * Acceptable as it is likely our actual input is also transposed.
      */

    println("Turning RDD into DenseMatrix")
    val rt = new DenseMatrix(265, 30, arr.take(7950))
    println("Finished turning RDD into DenseMatrix")
    val r = rt.t

    // Turn matrix into RDD to allow us to utilise MLlib SVD on it
    println("Creating RowMatrix")
    val rm: RowMatrix = new RowMatrix(matrix_To_RDD(rt, sc)) // We are feeding in a transpose matrix, so we only need V
    val numSamples = 29 // n - 1
    println("RowMatrix created ")

    var t0 = System.nanoTime()
    println("Start SVD")
    // we do not need u, since SVD is done on the transpose
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rm.computeSVD(Math.min(rm.numRows(), rm.numCols()).toInt, computeU = false)
    println("Done SVD")
    var t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "seconds")

    /**
      * SVD is done in a distributed manner. All operations are done locally, from here onwards.
      * Do we want to make this distributed?
      */

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
    for ( a <- toRemove.indices ) {
      if (toRemove(a)) {
        // c * ct * R, where R is the original, samples by targets, matrix
        val c = dmV(::, a); val ct = c.t
        val res = c * ct * r
        normR -= res
      }
    }

    // Turn each row in normR into a z-score
    val normZ = normR
    for ( a <- 0 until normR.rows ) {
      val row = normZ(a, ::); val cols = normZ.cols

      var total = 0.0
      for ( b <- 0 until cols ) {
        total += row(b)
      }
      val mean = total / cols

      var std = 0.0
      for ( b <- 0 until cols ) {
        std += Math.pow(row(b) - mean, 2)
      }
      std = Math.sqrt(std / (cols-1))

      for ( b <- 0 until cols ) {
        normZ(a,b) = (normZ(a,b) - mean) / std
      }
    }

    // Initialise our model
    Model.states = List("Diploid", "Duplication", "Deletion")
    Model.start = {
      case "Diploid" => 1.0
      case "Duplication" => 0.00000001
      case "Deletion" => 0.00000001
    }

    // Generate obs for each row
    for ( r <- 0 until normZ.rows ) {
      val listBuffer = new ListBuffer[Double]()
      for ( a <- 0 until normZ.cols ) {
        listBuffer += normZ(r, a)
      }

      // Note, this will currently fail if there are no obs in the row.
      Model.obs = listBuffer.toList // Set obs to row from Z matrix

//      if (r == 18 || r == 14) {

        Model.calc_probabilities_for_sample() // Calculate transition and emission probabilities
//        println("Running Viterbi..")
        t0 = System.nanoTime()
        val path = viterbi(obs, states, start, transitions, emissions)
//        println(phred_state_from_t1_to_t2(3, 10, "Duplication"))
        t1 = System.nanoTime()
//        println("Done Viterbi")
//        println("Elapsed time: " + (t1 - t0)/1000000000.0 + "seconds")

        print("r: " + r + " ")
//        print(path)
        for (a <- path) {
          if (a.equals("Deletion")) print(0 + " ")
          if (a.equals("Diploid")) print(1 + " ")
          if (a.equals("Duplication")) print(2 + " ")
//          println(a)
        }
        println()
//      }
    }
  }

  def matrix_To_RDD(m: DenseMatrix[Double], sc: SparkContext): RDD[Vector] = {
    val columns = m.toArray.grouped(m.rows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }
}

//      // Map: (State, Time) => Probability
//      var emission_probability : Map[(String, Int), Double] = Map()
//      // Generate emission probabilities for each sample
//      for ( a <- 0 to obs.length-1 ) {
//        val cur = obs(a)
//        emission_probability += ( ("Diploid", a) -> normDistDip.density(cur) )
//        emission_probability += ( ("Duplication", a) -> normDistDup.density(cur) )
//        emission_probability += ( ("Deletion", a) -> normDistDel.density(cur) )
//      }


// Table 1
//    val transition_probability_2 : (String, String) => Double = {
//      case ("Diploid", "Diploid")     => 1 - 2*p
//      case ("Diploid", "Duplication") => p
//      case ("Diploid", "Deletion")    => p
//
//      case ("Duplication", "Duplication") => 1 - q
//      case ("Duplication", "Diploid")     => q
//      case ("Duplication", "Deletion")    => 0.0
//
//      case ("Deletion", "Deletion")    => 1 - q
//      case ("Deletion", "Diploid")     => q
//      case ("Deletion", "Duplication") => 0.0
//    }

//    val normDistDip = new NormalDistribution(0.0, 1.0)
//    val normDistDel = new NormalDistribution(-m, 1.0)
//    val normDistDup = new NormalDistribution(m, 1.0)
