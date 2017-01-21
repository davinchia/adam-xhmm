/**
  * Created by davinchia on 1/19/17.
  */

import collection.mutable
import Types._
import Model._

object XHMM {
  private def forwardE(t: Target, j: State): Double = {
    val key = (t, j)
    if (fwdCache contains key) {
      fwdCache(key)
    } else {
      val result = if (t > 0) {
        Math.log(states map { i =>
          Math.exp(Math.log(forwardE(t - 1, i)) + Math.log(transitions(t, i, j)))
        } sum) + Math.log(emissions(j, t))
      } else {
        Math.log(start(j)) + Math.log(emissions(j, 0))
      }
      fwdCache(key) = Math.exp(result)
      Math.exp(result)
    }
  }

  private def backwardE(t: Target, i: State): Double = {
    val key = (t, i)
    if (bckCache contains key) {
      bckCache(key)
    } else {
      // This does not currently support t = 0
      val result = if (t < obs.length) {
        Math.log(states map { j =>
          Math.exp(Math.log(transitions(t, i, j)) + Math.log(emissions(j, t)) + Math.log(backwardE(t + 1, j)))
        } sum)
      } else {
        Math.log(1.0)
      }
      bckCache(key) = Math.exp(result)
      Math.exp(result)
    }
  }

  private def gamma(t: Target): BigDecimal = {
    BigDecimal(states map { s => forwardE(t, s) * backwardE(t+1, s)} sum)
  }

  def prob_state_from_t1_to_t2_numerator(t1: Target, t2: Target, state: State) : BigDecimal = {
    val fwd = forwardE(t1, state)
    val bck = backwardE(t2+1, state)
    //    println("fwd: " + fwd)
    //    println("bck: " + bck)
    //    println()

    val numerator = { (t1+1 to t2) map { (t) => { transitions(t, state, state) * emissions(state, t) } } }.product * fwd * bck

    //    for (t <- t1+1 to t2) {
    //      println("trans: " + transitions(t, state, state) )
    //      println("emiss: " + emissions(state, t) )
    //    }

    println("indiv: " + numerator)
    BigDecimal(numerator)
  }

  def calc_total_likelihood(): BigDecimal = {
    states map { (s) => { BigDecimal(forwardE(obs.length-1, s)) * BigDecimal(backwardE(obs.length, s)) } } sum
  }

  def calc_phred_score(score: Double): Double = {
    val scr = Math.round(-10 * Math.log10(score))
    if (scr < maxPhredScore) scr
    else maxPhredScore
  }


  /**
    * The following are higher level XHMM calls. These access the lower probability calls above.
    */
  def viterbi(observations: List[Double],
              states: List[State],
              start: State => Probability,
              transition: ProbabilityMap,
              emissions: mutable.Map[(State, Target), Double]): List[State] = {

    def probability(p: ProbabilityPath) = p._1

    def mostLikelyPath() : List[String] = {
      var path : mutable.ListBuffer[State] = mutable.ListBuffer[State]()
      for (t <- 0 to 264) {
        path += (states map { (s) => (Math.log(forwardE(t, s)) + Math.log(backwardE(t+1, s)), s)
        } maxBy (_._1))._2
      }
      path.toList
    }

    mostLikelyPath()
  }

  /**
    * The following are used to calculate quality statistics.
    */

  def exact_score(t1: Target, t2: Target, state: State) : Double = {
    val num = prob_state_from_t1_to_t2_numerator(t1, t2, state)
    val total = calc_total_likelihood()
    calc_phred_score(((total - num) / total).toDouble)
  }

  def some_score(t1: Target, t2: Target, exclude: State): Double = {
    for (t <- t1 to t2) {
      states foreach { s => fwdCache -= ((t, s)) }  // clear for recalculation
      emissions((exclude, t)) = 0.0                 // zone out probabilities
      states foreach { s => forwardE(t,s) }
    }

    val num : BigDecimal = gamma(t2)
    val subtr : BigDecimal = prob_state_from_t1_to_t2_numerator(t1, t2, "Diploid")
    var total : BigDecimal = calc_total_likelihood()

    println("num: " + num)
    println("subtr: " + subtr)
    println("total: " + total)
    println("final: " + (total - (num - subtr)) / total)

    calc_phred_score(((total - (num - subtr)) / total).toDouble)
  }

  def main(args: Array[String]): Unit = {

  }
}
