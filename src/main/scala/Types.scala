import scala.collection.Map

/**
  * Created by davinchia on 1/19/17.
  */
object Types {
  type State = String
  type Observation = String
  type Probability = Double
  type ProbabilityMap = (State, State) => Probability
  type EmissionMap = Map[(String, Int), Double]
  type ProbabilityPath = (Probability, List[State])
}
