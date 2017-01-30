/**
  * Created by joey on 1/29/17.
  */
class Sample(val observations: Array[Double], val len: Int) extends Serializable {
  var emissions   : Array[Array[BigDecimal]] = Array[Array[BigDecimal]]()  //[time, state]
  var forward     : Array[Array[BigDecimal]] = Array[Array[BigDecimal]]()  //[state, time]
  var backward    : Array[Array[BigDecimal]] = Array[Array[BigDecimal]]()  //[state, time]
  var viterbiPath : Array[Int]           = Array[Int]()

  def obs     = observations

  def emiss   = emissions
  def fwd     = forward
  def bck     = backward
  def vitPath = viterbiPath

  override def toString: String =
    "(observations: " + obs.mkString(", ") + " emission: " + emiss.deep.mkString(", ") +
      " forward: " + fwd.deep.mkString(", ") + " backward: " + bck.deep.mkString(", ") +
      "Viterbi Path: " + vitPath.mkString(", ")+ ")"
}