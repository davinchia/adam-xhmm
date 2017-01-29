/**
  * Created by davinchia on 1/13/17.
  */

import scala.collection.mutable.ListBuffer
import breeze.linalg.DenseMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import Model._
import XHMM._
import Utils._

object Main {
  def main(args: Array[String]) {

    /**
      * Spark is configured to run using Standalone Cluster mode.
      */
    val conf = new SparkConf().setAppName("Simple Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", "MyRegistrator")

    val sc = new SparkContext(conf)

    if (debug) println("Reading file.. ")
    // Input
//    val arr = sc.textFile("file:///home/joey/Desktop/1000 Genomes/run-results/test3/DATA.filtered_centered.RD.txt")
    val arr = sc.textFile("file:///home/joey/Desktop/1000 Genomes/RUN/DATA.filtered_centered.RD.txt")
    arr.cache() // Make sure Spark saves this to memory, since we are going to do more operations on it

    // Assign targets
    targets = arr.first.split("\\s+").drop(1)

    // Get rest of samples by targets from matrix; we must use higher-order functions because arr is a RDD
    var data = arr.filter(line => !line.contains("Matrix"))
    val rows = targets.length; val cols = data.count().toInt  // Prepare to transpose Matrix

    // Remove metadata and convert to Double
    var numericData = data.flatMap(_.split("\\s+")).filter(e => !e.contains("HG")).map(_.toDouble)
    var dataLength = rows * cols
    println("Finished reading file")

    var t0 = System.nanoTime(); var t1 = System.nanoTime(); var t2 = System.nanoTime(); var t3 = System.nanoTime(); var t4 = System.nanoTime()
    /**
      * To be clear, the DenseMatrix we use belongs to Breeze and not MLlib.
      * Breeze allows us to easily do matrix addition with transposes.
      * All DenseMatrices in this file refers to Breeze DenseMatrices; will be indicated otherwise if they refer to MLLib.
      * DenseMatrix reads in the matrix in column-major order; our matrix is being transposed.
      * Acceptable since this allows us to not calculate the U component.
      */

    if (debug) println("Turning RDD into DenseMatrix")
    val rt = new DenseMatrix(rows, cols, numericData.take(dataLength)); val r = rt.t
    println("Finished turning RDD into DenseMatrix")

    // Turn matrix into RDD to allow us to utilise MLlib SVD on it
    if (debug) println("Creating RowMatrix")
    val rm: RowMatrix = new RowMatrix(matrix_To_RDD(rt, sc)) // We are feeding in a transpose matrix, so we only need V
    val numSamples = cols
    println("RowMatrix created ")

    t1 = System.nanoTime()
    if (debug) println("Start SVD")
    // we do not need u, since SVD is done on the transpose
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rm.computeSVD(Math.min(rm.numRows(), rm.numCols()).toInt, computeU = false)
    println("Done SVD")
    if (debug) {
      t2 = System.nanoTime()
      println("Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    /**
      * SVD is done in a distributed manner. All operations are done locally, from here onwards.
      */

    if (debug) println("Starting PCA normalisation")
    t1 = System.nanoTime()
    val d: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
    val dmV : DenseMatrix[Double] = new DenseMatrix(V.numCols, V.numRows, V.toArray) // Transform V to DenseMatrix

    t3 = System.nanoTime()
    // Calculate total, relative values, and eigenvectors to remove
    val toRemove = calc_vectors_to_remove(d, numSamples)
    if (debug) {
      t4 = System.nanoTime()
      println("  Time to compute relative values: " + (t4 - t3)/1000000000.0 + " seconds")
      println("  Number of components to remove: " + toRemove.length)
    }

    // Equation 1
    t3 = System.nanoTime()
    var normR = r
    for ( a <- toRemove.indices ) {
      if (toRemove(a)) {
        // c * ct * R, where R is the original, samples by targets, matrix
        val c = dmV(::, a); val ct = c.t
        val res = c * ct * r
        normR -= res
      }
    }
    if (debug) {
      t4 = System.nanoTime()
      println("  Time to remove components: " + (t4 - t3)/1000000000.0 + " seconds")
      t2 = System.nanoTime()
      println("Done PCA normalisation")
      println("Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }


    if (debug) println("Starting Z-score normalisation")
    // Turn each row in normR into a z-score
    t1 = System.nanoTime()
    val normZ = z_normalise_matrix(normR)

    if (debug) {
      t2 = System.nanoTime()
      println("Done Z-score normalisation")
      println("Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    // Initialise our model
    calc_common_variables()

    var broadTrans = sc.broadcast(transitions)

    // Generate obs for each row
    for ( r <- 0 until normZ.rows ) {
      val listBuffer = new ListBuffer[Double]()
      for ( a <- 0 until normZ.cols ) {
        listBuffer += normZ(r, a)
      }

      Model.obs = listBuffer.toArray // Set obs to row from Z matrix
      println("Sample: " + (r+1))
      Model.calc_probabilities_for_sample()

      t2 = System.nanoTime()
      val path = viterbi()
      t3 = System.nanoTime()

      if (debug) {
        println("  Done Viterbi")
        println("  Elapsed time: " + (t3 - t2)/1000000000.0 + " seconds")
      }

      val (beginIdx, endIdx, state) = search_for_non_diploid(path)
      println("  " + beginIdx + ", " + endIdx + ", " + state)
    }
    t1 = System.nanoTime()
    println("Total time taken: " + ((t1 - t0)/1000000000.0) + " seconds")
  }
}

//      if (r == 18 || r == 14) {

//        if (beginIdx != -1) {
//          var exclude = if (state == 1) 2 else 1
//
//          val someScore = some_score(beginIdx, endIdx, exclude)
//          if (someScore >= minSomeScore) {
//            println("Num: " + r + " CNV: " + state + " begin: " + (beginIdx+1) + " end: " + (endIdx+1)
//              + " exact score: " + exact_score(beginIdx, endIdx, state) + " some score: " + someScore)
//          }
//        }
//        println("  Total Elapsed time: " + (t2 - t0)/1000000000.0 + "seconds")