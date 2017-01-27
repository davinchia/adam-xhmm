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
    val idxDel = path.indexOf("Deletion")
    val idxDup = path.indexOf("Duplication")

    if (idxDel >= 0 && idxDup >= 0) {
      println("Error. Path has multiple deviating states.")
      (-1, -1, "")
    } else if (idxDel < 0 && idxDup < 0) {
      (-1, -1, "")
    } else {
      var startIdx = idxDel; var endIdx = path.lastIndexOf("Deletion");
      var state = "Deletion"

      if (idxDup >= 0) {
        startIdx = idxDup; endIdx = path.lastIndexOf("Duplication")
        state = "Duplication"
      }

      for (a <- path.slice(startIdx, endIdx+1)) {
        if (a != state) {
          println("Error. Path has multiple breaks.")
          return (-1, -1, "")
        }
      }
      (startIdx, endIdx, state)

    }
  }

  def ugly_print_path(path: List[State]): Unit = {
    for (a <- path) {
      if (a == "Deletion") print(0 + " ")
      if (a == "Diploid") print(1 + " ")
      if (a == "Duplication") print(2 + " ")
    }
  }

  def print_emiss(): Unit = {
    for (t <- 0 to 264) {
      println("t: " + t + " " + emissions("Deletion", t) + " " + emissions("Diploid", t) + " " + emissions("Duplication", t))
    }
  }

  def print_trans(): Unit = {
    for (t <- 1 to 264) {
      println("t: " + t)
      println("state: " + "Del" + " " + transitions(t, "Deletion", "Deletion") + " " + transitions(t, "Deletion", "Diploid") + " " + transitions(t, "Deletion", "Duplication"))
      println("state: " + "Dip" + " " + transitions(t, "Diploid", "Deletion") + " " + transitions(t, "Diploid", "Diploid") + " " + transitions(t, "Diploid", "Duplication"))
      println("state: " + "Dup" + " " + transitions(t, "Duplication", "Deletion") + " " + transitions(t, "Duplication", "Diploid") + " " + transitions(t, "Duplication", "Duplication"))
    }
  }

  def main(args: Array[String]): Unit = {
    println(" Pass: ")
    val pass1 = List("Diploid", "Diploid", "Diploid", "Deletion", "Diploid", "Diploid")
    val pass2 = List("Diploid", "Diploid", "Diploid", "Duplication", "Diploid", "Diploid")
    val pass3 = List("Diploid", "Diploid", "Diploid", "Deletion", "Deletion", "Deletion")
    val pass4 = List("Diploid", "Duplication", "Duplication", "Duplication", "Diploid", "Diploid")
    val pass5 = List("Deletion", "Deletion", "Diploid", "Diploid", "Diploid", "Diploid")

    println(search_for_non_diploid(pass1)) // (3, 3, Deletion)
    println(search_for_non_diploid(pass2)) // (3, 3, Duplication)
    println(search_for_non_diploid(pass3)) // (3, 5, Deletion)
    println(search_for_non_diploid(pass4)) // (1, 3, Duplication)
    println(search_for_non_diploid(pass5)) // (0, 1, Deletion)

    println(" Fail: ")
    val fail1 = List("Deletion", "Duplication", "Diploid", "Diploid", "Diploid", "Diploid") // Multiple states
    val fail2 = List("Deletion", "Diploid", "Deletion", "Diploid", "Diploid", "Diploid")    // Multiple breaks
    val fail3 = List("Diploid", "Duplication", "Duplication", "Deletion", "Diploid", "Diploid") // Multiple states
    val fail4 = List("Diploid", "Duplication", "Diploid", "Duplication", "Diploid", "Diploid") // Multiple breaks
    val fail5 = List("Duplication", "Duplication", "Diploid", "Duplication", "Diploid", "Duplication")

    println(search_for_non_diploid(fail1)) // Multiple states
    println(search_for_non_diploid(fail2)) // Multiple breaks
    println(search_for_non_diploid(fail3)) // Multiple states
    println(search_for_non_diploid(fail4)) // Multiple breaks
    println(search_for_non_diploid(fail5)) // Multiple breaks

    println(" Normal: ")
    val normal = List("Diploid", "Diploid", "Diploid", "Diploid", "Diploid")
    println(search_for_non_diploid(normal))
  }
}
