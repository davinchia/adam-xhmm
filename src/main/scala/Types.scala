/**
  * Created by davinchia on 1/19/17.
  */

import collection.mutable

object Types {
  type State = String
  type Target = Int
  type Observation = String
  type Probability = BigDecimal
  type ProbabilityMap = mutable.Map[(Int, State, State), BigDecimal]
  type EmissionMap = mutable.Map[(String, Int), BigDecimal]
  type ProbabilityPath = (Probability, List[State])
}
