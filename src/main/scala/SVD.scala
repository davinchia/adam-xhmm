/**
  * Created by davinchia on 1/13/17.
  */

import scala.collection.mutable.ListBuffer
import breeze.linalg.DenseMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import Model._
import XHMM._
import Utils._

object SVD {
  def main(args: Array[String]) {

    /**
      * Spark is configured to run using Standalone Cluster mode.
      */

    val conf = new SparkConf().setAppName("Simple Application")
//      .setMaster("spark://davins-air.middlebury.edu:7077")
//      .setMaster("local[2]")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)

    println("Reading file.. ")
    // Input
//    val arr = sc.textFile("file:///Users/davinchia/Desktop/Projects/xhmm/RUN/DATA.filtered_centered.RD.txt")
    val arr = sc.textFile("file:///home/joey/Desktop/1000 Genomes/run-results/test3/DATA.filtered_centered.RD.txt")
    arr.cache() // Make sure Spark saves this to memory, since we are going to do more operations on it

    // Assign targets
    targets = arr.first.split("\\s+").drop(1)

    // Get rest of samples by targets from matrix; we must use higher-order functions because arr is a RDD
    var data = arr.filter(line => !line.contains("Matrix"))

    // Prepare to transpose Matrix
    val rows = targets.length
    val cols = data.count().toInt

    // Remove metadata and convert to Double
    var numericData = data.flatMap(_.split("\\s+")).filter(e => !e.contains("HG")).map(_.toDouble)
    var dataLength = rows * cols
    println("Finished reading file")

    var t0 = System.nanoTime(); var t1 = System.nanoTime(); var t2 = System.nanoTime(); var t3 = System.nanoTime();
    /**
      * To be clear, the DenseMatrix we use belongs to Breeze and not MLlib.
      * Breeze allows us to easily do matrix addition with transposes.
      * All DenseMatrices in this file refers to Breeze DenseMatrices; will be indicated otherwise if they refer to MLLib.
      * DenseMatrix reads in the matrix in column-major order; our matrix is being transposed.
      * Acceptable since this allows us to not calculate the U component.
      */

    println("Turning RDD into DenseMatrix")
    val rt = new DenseMatrix(rows, cols, numericData.take(dataLength))
    println("Finished turning RDD into DenseMatrix")
    val r = rt.t

    // Turn matrix into RDD to allow us to utilise MLlib SVD on it
    println("Creating RowMatrix")
    val rm: RowMatrix = new RowMatrix(matrix_To_RDD(rt, sc)) // We are feeding in a transpose matrix, so we only need V
    val numSamples = cols
    println("RowMatrix created ")

    t1 = System.nanoTime()
    println("Start SVD")
    // we do not need u, since SVD is done on the transpose
    println("rows: " + rm.numRows()); println("cols: " + rm.numCols())
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rm.computeSVD(Math.min(rm.numRows(), rm.numCols()).toInt, computeU = false)
    println("Done SVD")
    t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1)/1000000000.0 + "seconds")
    println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + "seconds")

    /**
      * SVD is done in a distributed manner. All operations are done locally, from here onwards.
      */

    println("Starting PCA normalisation")
    t1 = System.nanoTime()
    val d: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
    val dmV : DenseMatrix[Double] = new DenseMatrix(V.numCols, V.numRows, V.toArray) // Transform V to DenseMatrix

    val vals = d.toArray.map( x => x * x / numSamples ) // Transform singular values into eigenvalues

    // Calculate total, relative values, and eigenvectors to remove
    val total = vals.sum
    val relVar = vals.map( x => x / total )
    val toRemove = relVar.map( x => x >= 0.7 / numSamples )

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
    t2 = System.nanoTime()
    println("Done PCA normalisation")
    println("Elapsed time: " + (t2 - t1)/1000000000.0 + "seconds")
    println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + "seconds")


    println("Starting Z-score normalisation")
    // Turn each row in normR into a z-score
    t1 = System.nanoTime()
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
    t2 = System.nanoTime()
    println("Done Z-score normalisation")
    println("Elapsed time: " + (t2 - t1)/1000000000.0 + "seconds")
    println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + "seconds")

    println("Initialising model..")
    // Initialise our model
    Model.states = List("Diploid", "Duplication", "Deletion")
    Model.start = {
      case "Diploid" => 1.0
      case "Duplication" => 0.00000001
      case "Deletion" => 0.00000001
    }
    calc_transition_probabilities()

    println("Starting processing for each sample..")
    // Generate obs for each row
    for ( r <- 0 until normZ.rows ) {
      val listBuffer = new ListBuffer[Double]()
      for ( a <- 0 until normZ.cols ) {
        listBuffer += normZ(r, a)
      }

      // Note, this will currently fail if there are no obs in the row.
      Model.obs = listBuffer.toList // Set obs to row from Z matrix
      println("Sample: " + (r+1))
//      if (r == 18 || r == 14) {
        Model.calc_probabilities_for_sample() // Calculate transition and emission probabilities

        println("  Running Viterbi..")
        t2 = System.nanoTime()
        val path = viterbi(obs, states, start, transitions, emissions)
        t3 = System.nanoTime()
        println("  Done Viterbi")
        println("  Elapsed time: " + (t3 - t2)/1000000000.0 + "seconds")

        val (beginIdx, endIdx, state) = search_for_non_diploid(path)
        if (beginIdx != -1) {
          var exclude = if (state == "Deletion") "Duplication" else "Deletion"

          val someScore = some_score(beginIdx, endIdx, exclude)
          if (someScore >= minSomeScore) {
            println("Num: " + r + " CNV: " + state + " begin: " + (beginIdx+1) + " end: " + (endIdx+1)
              + " exact score: " + exact_score(beginIdx, endIdx, state) + " some score: " + someScore)
          }
        }
        println("  Total Elapsed time: " + (t2 - t0)/1000000000.0 + "seconds")
      }
    t1 = System.nanoTime()
    println("Total time taken: " + ((t1 - t0)/1000000000.0) + "seconds")
  }
}
