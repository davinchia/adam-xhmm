/**
  * Created by davinchia on 1/15/17.
  *
  */

import scala.collection.{Map, mutable}
import Types._
import ForwardBackward._

object Viterbi {

  /**
    * This implementation of Viterbi only calculates the most probable state path till the end.
    */
  def viterbi(observations: List[Double],
              states: List[State],
              start: State => Probability,
              transition: ProbabilityMap,
              emissions: Map[(State, Target), Double]): List[State] = {

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
}
