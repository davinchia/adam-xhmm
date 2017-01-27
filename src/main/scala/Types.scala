/**
  * Created by davinchia on 1/19/17.
  */

import collection.mutable

object Types {
  type State = Int
  type Target = Int
  type Observation = String
  type Probability = BigDecimal
  type ProbabilityMap = Array[Array[Array[BigDecimal]]]
  type ProbabilityPath = (Probability, List[State])
}
