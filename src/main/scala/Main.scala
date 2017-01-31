/**
  * Created by davinchia on 1/13/17.
  */

import breeze.linalg.{ DenseMatrix => BreezeDenseMatrix }
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{ Vector, Matrix, SingularValueDecomposition }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import Model.{calc_common_variables, targets, transitions}
import Utils._
import org.apache.spark.rdd.RDD
import DistXHMM._

import scala.collection.mutable


object Main {
  def main(args: Array[String]) {
    val fakeData = false
    /**
      * Spark is configured to run using Standalone Cluster mode.
      */
    val conf = new SparkConf().setAppName("DECA")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", "MyRegistrator")

    val sc = new SparkContext(conf); val fakeLength = 2000

    if (debug) println("Reading file.. ")
    // Input
//    val arr = sc.textFile("/user/dchia/DATA.filtered_centered.RD.txt")
//    val arr = sc.textFile("/user/dchia/DATA.filtered_centered.small.RD.txt")
//    val arr = sc.textFile("file:///home/joey/Desktop/1000 Genomes/RUN/DATA.filtered_centered.RD.txt")
    val arr = sc.textFile("file:///home/joey/Desktop/1000 Genomes/run-results/test3/DATA.filtered_centered.RD.txt")
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
      * Slightly confusing but we are using both Breeze and Spark dense matrixes. They have been renamed to reflect that.
      * We do so because Breeze offers an easier way to convert and multiply data, while we need Spark for the multiplcation
      * step in PCA normalisation.
      * DenseMatrix reads in the matrix in column-major order; our matrix is being transposed.
      */

    if (debug) println("Turning RDD into DenseMatrix")
    val rt = new BreezeDenseMatrix(rows, cols, numericData.take(dataLength)); val r = rt.t
    println("Finished turning RDD into DenseMatrix")

    println(r.rows + " " + r.cols)

    // Turn matrix into RDD to allow us to utilise MLlib SVD on it
    if (debug) println("Creating RowMatrix")
    val rm: RowMatrix = new RowMatrix(matrix_To_RDD(rt, sc))
    val numSamples = cols
    println("RowMatrix created ")

    t1 = System.nanoTime()
    if (debug) println("Start SVD")
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rm.computeSVD(Math.min(rm.numRows(), rm.numCols()).toInt, computeU = true)
    println("Done SVD")
    if (debug) {
      t2 = System.nanoTime()
      println("Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    if (debug) println("Starting PCA normalisation")
    t1 = System.nanoTime()
    // We need to compute U in order to normalise the matrix using the PCA components.
    val U: RowMatrix = svd.U // The U factor is a rowmatrix
    val d: Vector    = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix    = svd.V // The V factor is a local dense matrix.

    // Transform U & V into DenseMatrix for easier operations. Transpose because Breeze takes in column-major arrays.
    val dmVt : BreezeDenseMatrix[Double] = new BreezeDenseMatrix(V.numRows, V.numCols, V.toArray).t
    var dmU  : BreezeDenseMatrix[Double] = BreezeDenseMatrix.zeros[Double](U.numRows().toInt, U.numCols().toInt)
    var row = 0
    U.rows.collect().foreach{
      Vect => {
        for (col <- 0 until numSamples) {
          dmU(row, col) = Vect(col)
        }
        row += 1
      }
    }

    t3 = System.nanoTime()
    val toRemove = calc_vectors_to_remove(d, numSamples)
    if (debug) {
      t4 = System.nanoTime()
      println("  Time to compute relative values: " + (t4 - t3)/1000000000.0 + " seconds")
      println("  Number of components to remove: " + toRemove.filter(x => x).length)
    }

    // Remove the Principal Components with the highest variance according to our threshold.
    val zeroD: BreezeDenseMatrix[Double] = BreezeDenseMatrix.zeros[Double](numSamples, numSamples)
    // Zero out the components we are removing.
    for (i <- 0 until toRemove.length) {
      if (!toRemove(i)) zeroD(i,i) = d(i)
      }
    val normR = (dmU * zeroD * dmVt).t

    t3 = System.nanoTime()

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
    val normZ: BreezeDenseMatrix[Double] = z_normalise_matrix(normR)
    if (debug) {
      t2 = System.nanoTime()
      println("Done Z-score normalisation")
      println("Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    // Initialise our model
    calc_common_variables()

    var broadTrans = sc.broadcast(transitions)
    println("Start parallelising..")

    var fakeSamples = new mutable.ListBuffer[Sample]
    var obsArray = (1 to transitions.length).toArray.map(_.toDouble)
    for (i <- 1 to fakeLength) {
      fakeSamples += new Sample(obsArray, obsArray.length)
    }

    var samples: RDD[Sample] = if (fakeData) sc.parallelize(fakeSamples.toArray[Sample])
                                else  sc.parallelize(convert_to_sample_array(normZ))
    samples.cache()

    t1 = System.nanoTime()
    samples = samples.map( e => {
      e.emissions   = e.observations map ( a => calc_emission(a) )
      e.forward     = calc_forward(e.observations.length, e.emissions, broadTrans.value)
      e.backward    = calc_backward(e.observations.length, e.emissions, broadTrans.value)
      e.viterbiPath = calc_viterbi(e.fwd, e.bck)
      e
    })

    val viterbis = samples.map(e => {e.viterbiPath}).collect()
    t2 = System.nanoTime()
    println("Done Viterbi for: " + viterbis.length)
//    viterbis.foreach( e => println(e.deep.mkString(", ")))
//    println("Elapsed time: " + (t2 - t1)/1000000000 + " seconds")
    println("Done calculations.")

    println("Total Elapsed time: " + (t2 - t0)/1000000000 + " seconds")
  }
}

// Transform U & V into DenseMatrix for easier operations. Transpose because Breeze takes in column-major arrays.
//val dmU = new BreezeDenseMatrix(U.numRows().toInt, U.numCols().toInt, U.rows.map(x => x.toArray).collect.flatten).t
//val dmVt : BreezeDenseMatrix[Double] = new BreezeDenseMatrix(V.numRows, V.numCols, V.toArray).t
//
//
//
//t3 = System.nanoTime()
//val toRemove = calc_vectors_to_remove(d, numSamples)
//if (debug) {
//  t4 = System.nanoTime()
//  println("  Time to compute relative values: " + (t4 - t3)/1000000000.0 + " seconds")
//  println("  Number of components to remove: " + toRemove.filter(x => x).length)
//}

//var normR = r
//for ( a <- toRemove.indices ) {
//  if (toRemove(a)) {
//    // c * ct * R, where R is the original, samples by targets, matrix
//    val c = dmV(::, a); val ct = c.t
//    val res = c * ct * r
//    normR -= res
//  }
//}


// Generate obs for each row
//for ( r <- 0 until normZ.rows ) {
//  val listBuffer = new ListBuffer[Double]()
//  for ( a <- 0 until normZ.cols ) {
//    listBuffer += normZ(r, a)
//  }
//
//  Model.obs = listBuffer.toArray // Set obs to row from Z matrix
//  println("Sample: " + (r+1))
//  Model.calc_probabilities_for_sample()
//
//  t2 = System.nanoTime()
//  val path = viterbi()
//  t3 = System.nanoTime()
//
//  if (debug) {
//    println("  Done Viterbi")
//    println("  Elapsed time: " + (t3 - t2)/1000000000.0 + " seconds")
//  }
//
//  val (beginIdx, endIdx, state) = search_for_non_diploid(path)
//  println("  " + beginIdx + ", " + endIdx + ", " + state)
//}
//t1 = System.nanoTime()
//println("Total time taken: " + ((t1 - t0)/1000000000.0) + " seconds")
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