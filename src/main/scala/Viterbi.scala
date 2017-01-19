/**
  * Created by davinchia on 1/15/17.
  *
  *   Scala Implementation of Viterbi's Algorithm Adapted from:
  *   https://gist.github.com/dansondergaard/4144770
  *     - Added dynamic table for memoisation
  *     - Converted equations to log functions to prevent underflow
  *
  *
  */
import scala.collection.Map
import Types._

object Viterbi {
  var seenProbMap : Map[(State, Int), ProbabilityPath] = Map()

  def viterbi(observations: List[Double],
              states: List[State],
              start: State => Probability,
              transition: ProbabilityMap,
              emissions: Map[(String, Int), Double]): ProbabilityPath = {

    def probability(p: ProbabilityPath) = p._1

    def mostLikelyPathFrom(state: State, time: Int): ProbabilityPath = {
      if (seenProbMap.contains((state, time))) {
        return seenProbMap((state, time))
      }

      val emission = Math.log10(emissions((state, time)))
      time match {
        case 0 =>
          // (probability that were in the initial state) times
          // (probability of observing the initial observation from the initial state)
          val pPath = ( Math.log10(start(state)) + emission, List(state) )
          seenProbMap += ( (state, time) -> pPath )
          return pPath
        case _ =>
          val (prob, path) = states map { (state) =>
            val (prob, path) = mostLikelyPathFrom(state, time - 1)
            val prevState = path.head
            // (probability of the previous state) times
            // (probability of moving from previous state to this state)
            ( prob + Math.log10(transition(prevState, state)), path )
          } maxBy probability
          // (probability of observing the current observation from this state) times
          // (probability of the maximizing state)
          val pPath = (emission + prob, state :: path)
          seenProbMap += ( (state, time) -> pPath )
          return pPath
      }
    }

    val (prob, path) = states map { (state) =>
      mostLikelyPathFrom(state, observations.size - 1)
    } maxBy probability

    (prob, path.reverse)
  }
}
