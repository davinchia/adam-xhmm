/**
  * Created by davinchia on 1/13/17.
  */


import DistXHMM.{ calc_forward, calc_emission, calc_backward, calc_viterbi }
import Model.{calc_common_variables, targets, transitions}
import Utils._
import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    var path = args(0)
    println(path)

    /**
      * Spark is configured to run using Standalone Cluster mode.
      */
    val conf = new SparkConf().setAppName("DECA")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", "MyRegistrator")

    val sc = new SparkContext(conf);

    if (debug) println("Reading file.. ")
    // Input
    val arr = sc.textFile(path)
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
    val rt = new BreezeDenseMatrix(rows, cols, numericData.collect()); val r = rt.t
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
      println("  Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("  Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    if (debug) println("Starting PCA normalisation")
    t1 = System.nanoTime()
    // We need to compute U in order to normalise the matrix using the PCA components.
    val U: RowMatrix = svd.U // The U factor is a rowmatrix
    val d: Vector    = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix    = svd.V // The V factor is a local dense matrix.

    t3 = System.nanoTime()
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

    if (debug) {
      t4 = System.nanoTime()
      println("  Finished setting up PCA matrices")
      println("  Time to set up PCA matrixes: " + (t4 - t3)/1000000000.0 + " seconds")
    }

    t3 = System.nanoTime()
    val toRemove = calc_vectors_to_remove(d, numSamples)
    if (debug) {
      t4 = System.nanoTime()
      println("  Time to compute relative values: " + (t4 - t3)/1000000000.0 + " seconds")
      println("  Number of components to remove: " + toRemove.filter(x => x).length)
    }

    t3 = System.nanoTime()
    // Remove the Principal Components with the highest variance according to our threshold.
    val zeroD: BreezeDenseMatrix[Double] = BreezeDenseMatrix.zeros[Double](numSamples, numSamples)
    // Zero out the components we are removing.
    for (i <- 0 until toRemove.length) {
      if (!toRemove(i)) zeroD(i,i) = d(i)
      }
    val normR = (dmU * zeroD * dmVt).t

    if (debug) {
      t4 = System.nanoTime()
      println("  Time to remove components: " + (t4 - t3)/1000000000.0 + " seconds")
      t2 = System.nanoTime()
      println("Done PCA normalisation")
      println("  Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("  Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }


    if (debug) println("Starting Z-score normalisation")
    // Turn each row in normR into a z-score
    t1 = System.nanoTime()
    val normZ: BreezeDenseMatrix[Double] = z_normalise_matrix(normR)
    if (debug) {
      t2 = System.nanoTime()
      println("Done Z-score normalisation")
      println("  Elapsed time: " + (t2 - t1)/1000000000.0 + " seconds")
      println("  Total Elapsed time: " + (t2 - t0)/1000000000.0 + " seconds")
    }

    t1 = System.nanoTime()
    // Initialise our model
    calc_common_variables()

    var broadTrans = sc.broadcast(transitions)
    println("Start parallelising..")

    var samples: RDD[Sample] = sc.parallelize(convert_to_sample_array(normZ))
    samples.cache()
    println("Done parallelising")
    samples = samples.map( e => {
            e.emissions   = e.observations map ( a => calc_emission(a) )
            e.forward     = calc_forward(e.observations.length, e.emissions, broadTrans.value)
            e.backward    = calc_backward(e.observations.length, e.emissions, broadTrans.value)
            e.viterbiPath = calc_viterbi(e.fwd, e.bck)
            e
          })

    val viterbis = samples.count()

    t2 = System.nanoTime()
    println("  Done Viterbi for: " + viterbis)
    println("  Elapsed time: " + (t2 - t1)/1000000000 + " seconds")
    println("Done calculations.")

    println("Total Elapsed time: " + (t2 - t0)/1000000000 + " seconds")
  }
}