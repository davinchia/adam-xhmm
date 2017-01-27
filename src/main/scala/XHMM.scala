/**
  * Created by davinchia on 1/19/17.
  */

import collection.mutable
import Types._
import Model._
import Utils._

object XHMM {
  private def forward(): Unit = {
    states.foreach( e =>
      fwdCache( (0, e) ) = start(e) * emissions(e)(0)
    )

    for (i <- 1 to obs.length-1) {
      for (s <- states) {
        fwdCache( (i, s) ) = (states map { e =>
          fwdCache((i - 1, e)) * transitions(i)(e)(s)
        } sum) * emissions(s)(i)
      }
    }
  }

  private def forward(t: Target): Unit = {
    for (s <- states) {
      fwdCache((t, s)) = (states map { e =>
        fwdCache((t - 1, e)) * transitions(t)(e)(s)
      } sum) * emissions(s)(t)
    }
  }

  private def backward(): Unit = {
    states.foreach( e =>
      bckCache( (obs.length, e) ) = BigDecimal(1)
    )

    for (i <- (1 to obs.length-1).reverse) {
      for (s <- states) {
        bckCache( (i, s) ) =  states map { j =>
          transitions(i)(s)(j) * emissions(j)(i) * bckCache( (i+1 , j) )
        } sum
      }
    }

  }

  private def gamma(t: Target): BigDecimal = {
    states map { s => fwdCache(t, s) * bckCache(t+1, s)} sum
  }

  def calculate_Forward_Backward(): Unit = {
    forward()
    backward()
  }

  def prob_state_from_t1_to_t2_numerator(t1: Target, t2: Target, state: State) : BigDecimal = {
    val fwd = fwdCache((t1, state))
    val bck = bckCache((t2+1, state))
    val numerator = { (t1+1 to t2) map { (t) => { transitions(t)(state)(state) * emissions(state)(t) } } }.product * fwd * bck

    numerator
  }

  def calc_total_likelihood(): BigDecimal = {
    gamma(obs.length-1)
  }

  /**
    * The following are higher level XHMM calls. These access the lower probability calls above.
    */
  def viterbi(observations: List[Double],
              states: Array[State],
              start: State => BigDecimal,
              transition: ProbabilityMap,
              emissions: Array[Array[BigDecimal]]): List[State] = {

    def probability(p: ProbabilityPath) = p._1
    def mostLikelyPath() : List[State] = {
      var path : mutable.ListBuffer[State] = mutable.ListBuffer[State]()
      for (t <- 0 to 264) {
        path += (states map { (s) => (fwdCache((t, s)) * bckCache((t+1, s)), s)
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
      emissions(exclude)(t) = 0.0                 // zone out probabilities
      forward(t)                                    // recalculate probabilities
    }

    val num : BigDecimal = gamma(t2)
    val subtr : BigDecimal = prob_state_from_t1_to_t2_numerator(t1, t2, 1)
    var total : BigDecimal = calc_total_likelihood()

    calc_phred_score(((total - (num - subtr)) / total).toDouble)
  }

  def main(args: Array[String]): Unit = {

  }
}
