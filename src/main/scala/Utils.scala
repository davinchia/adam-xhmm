/**
  * Created by davinchia on 1/20/17.
  */

import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

import Model._
import Types._

object Utils {
  def matrix_To_RDD(m: DenseMatrix[Double], sc: SparkContext): RDD[Vector] = {
    val columns = m.toArray.grouped(m.rows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }

  def calc_phred_score(score: Double): Double = {
    val scr = Math.round(-10 * Math.log10(score))
    if (scr < maxPhredScore) scr
    else maxPhredScore
  }

  /**
    * Currently only deviations with a single break and state from Diploid are supported.
    * Valid Deviations:
    *   Dip Dip Dip Del Del Del Del Dip Dip
    *   Dip Dup Dip Dip Dip Dip Dip Dip Dip
    *   Dip Dip Dip Dip Dup Dup Dup Dup Dup
    *
    * Invalid Deviations:
    *   Dip Dip Dup Del Del Dip Dip Dip Dip (Multiple deviating states)
    *   Dip Dip Dup Dup Dip Dip Del Del Dip (Multiple breaks)
    *
    * A warning is printed if there are invalid deviations.
    */
  def search_for_non_diploid(path: List[State]): (Int, Int, State) = {
    val idxDel = path.indexOf(0)
    val idxDup = path.indexOf(2)

    if (idxDel >= 0 && idxDup >= 0) {
      println("Error. Path has multiple deviating states.")
      (-1, -1, -1)
    } else if (idxDel < 0 && idxDup < 0) {
      (-1, -1, -1)
    } else {
      var startIdx = idxDel; var endIdx = path.lastIndexOf(0)
      var state = 0

      if (idxDup >= 0) {
        startIdx = idxDup; endIdx = path.lastIndexOf(2)
        state = 2
      }

      for (a <- path.slice(startIdx, endIdx+1)) {
        if (a != state) {
          println("Error. Path has multiple breaks.")
          return (-1, -1, -1)
        }
      }
      (startIdx, endIdx, state)

    }
  }
}
