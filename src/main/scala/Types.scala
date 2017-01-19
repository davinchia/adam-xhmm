/**
  * Created by davinchia on 1/19/17.
  */

import collection.mutable

object Types {
  type State = String
  type Target = Int
  type Observation = String
  type Probability = Double
  type ProbabilityMap = mutable.Map[(Int, State, State), Double]
  type EmissionMap = mutable.Map[(String, Int), Double]
  type ProbabilityPath = (Probability, List[State])
}
