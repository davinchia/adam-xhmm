/**
  * Created by davinchia on 1/19/17.
  *
  *   This class serves as a container for the parameters needed for the current sample being operated on.
  *
  */

import Types._
import XHMM.calculate_Forward_Backward
import org.apache.commons.math3.distribution.NormalDistribution
import collection.mutable
import Utils.debug

object Model {
  val fwdCache: mutable.Map[(Target, State), BigDecimal] = mutable.HashMap[(Target, State), BigDecimal]()
  val bckCache: mutable.Map[(Target, State), BigDecimal] = mutable.HashMap[(Target, State), BigDecimal]()

  // The following are default values used to calculate certain probabilities.
  val p : BigDecimal = 0.00000001
  val t : BigDecimal = 6
  val q : BigDecimal = 1.0 / t
  val m : Int = 3
  val D : BigDecimal = BigDecimal(70000)  // BigDecimal to avoid underflow later on.
  val maxPhredScore : Int = 99            // Used to normalise Phred Score
  val minSomeScore : Double = 30

  // These are used to calculate Emission probabilities.
  val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
  val normDistDel : NormalDistribution = new NormalDistribution(-m, 1.0)
  val normDistDup : NormalDistribution = new NormalDistribution(m, 1.0)

  // Default model parameters
  var start : State => BigDecimal = {
    case 1 => 1.0
    case 2 => 0.00000001
    case 0 => 0.00000001
  }
  var states : Array[State] = Array(0, 1, 2)

  // These are used to calculate little d for each sample.
  // Targets is initially filled with a sample used in TestScript; this is set during the actual run.
  var targets : Array[String] = "22:16449025-16449223  22:16449225-16449423  22:16449425-16449804  22:17071768-17071966  22:17071968-17072166  22:17072168-17072366  22:17072368-17072566  22:17072568-17072766  22:17072768-17072966  22:17072968-17073166  22:17073168-17073440  22:17577952-17577976  22:17578688-17578833  22:17579666-17579777  22:17581246-17581371  22:17582886-17582933  22:17583030-17583192  22:17584385-17584467  22:17585617-17585700  22:17586481-17586492  22:17586744-17586841  22:17588617-17588658  22:17589198-17589396  22:17589398-17589596  22:17589598-17589796  22:17590198-17590396  22:17590398-17590710  22:17600282-17600480  22:17600882-17601080  22:17601082-17601091  22:17618912-17619247  22:17619441-17619628  22:17621950-17622123  22:17623988-17624021  22:17625915-17626007  22:17629339-17629450  22:17630433-17630635  22:17646099-17646134  22:17662374-17662466  22:17662711-17662912  22:17663495-17663651  22:17669230-17669337  22:17670833-17670922  22:17672574-17672700  22:17680439-17680468  22:17684454-17684663  22:17687962-17688180  22:17690247-17690567  22:18062711-18062730  22:18063864-18063925  22:18064124-18064179  22:18065330-18065419  22:18066183-18066300  22:18069903-18070067  22:18070692-18070845  22:18072356-18072431  22:18072862-18073002  22:18075440-18075502  22:18077296-18077382  22:18080961-18081054  22:18082793-18082861  22:18083859-18083947  22:18095578-18095644  22:18095978-18096086  22:18102227-18102292  22:18111369-18111401  22:18138479-18138598  22:18165981-18166087  22:18171753-18171908  22:18178907-18178976  22:18185010-18185152  22:18209444-18209642  22:18209644-18209842  22:18209844-18210042  22:18210044-18210300  22:18218346-18218357  22:18220784-18220995  22:18222115-18222254  22:18226570-18226779  22:18232871-18232940  22:18256377-18256455  22:18562641-18562780  22:18566204-18566498  22:18567879-18568024  22:18570739-18570841  22:18604247-18604468  22:18606924-18607071  22:18609122-18609320  22:18609322-18609520  22:18609522-18609801  22:18613611-18613903  22:18640432-18640587  22:18642940-18643035  22:18644558-18644702  22:18650023-18650101  22:18650658-18650803  22:18652612-18652706  22:18653521-18653687  22:18655918-18656048  22:18656560-18656609  22:18893889-18893997  22:18894079-18894238  22:18897686-18897785  22:18898402-18898541  22:18899054-18899202  22:18900689-18900875  22:18900952-18901039  22:18904404-18904501  22:18905830-18906004  22:18906965-18907110  22:18907220-18907311  22:18908856-18908936  22:18909839-18909917  22:18910331-18910446  22:18910628-18910692  22:18912565-18912713  22:18913201-18913235  22:18918504-18918711  22:19026379-19026634  22:19028572-19028807  22:19029321-19029472  22:19035954-19036156  22:19044500-19044675  22:19050715-19050791  22:19052362-19052580  22:19055614-19055738  22:19076882-19077003  22:19118914-19119112  22:19119114-19119312  22:19119314-19119512  22:19119514-19119712  22:19119714-19119989  22:19121710-19121988  22:19122574-19122688  22:19124837-19124945  22:19125729-19125830  22:19126673-19126805  22:19127126-19127242  22:19127369-19127537  22:19130052-19130146  22:19130240-19130407  22:19132020-19132153  22:19163644-19163757  22:19163934-19164007  22:19164092-19164206  22:19164360-19164463  22:19164634-19164717  22:19165241-19165378  22:19318964-19319079  22:19338882-19338969  22:19340880-19341042  22:19341520-19341641  22:19343284-19343388  22:19343753-19343811  22:19344414-19344574  22:19346860-19347007  22:19348761-19348864  22:19349251-19349454  22:19363155-19363315  22:19365393-19365589  22:19371144-19371228  22:19373045-19373259  22:19375235-19375339  22:19376007-19376077  22:19379625-19379737  22:19381866-19382032  22:19384311-19384470  22:19385516-19385610  22:19393310-19393403  22:19394708-19394797  22:19396007-19396116  22:19398239-19398301  22:19418963-19418999  22:19420789-19420871  22:19422260-19422417  22:19423162-19423485  22:19438193-19438267  22:19442273-19442353  22:19443204-19443291  22:19444110-19444157  22:19444376-19444396  22:19445594-19445662  22:19452725-19452797  22:19455397-19455526  22:19459211-19459331  22:19462591-19462623  22:19462994-19463125  22:19467492-19467542  22:19467681-19467740  22:19468477-19468568  22:19470214-19470350  22:19471386-19471528  22:19481850-19481905  22:19483504-19483552  22:19484909-19484970  22:19486624-19486674  22:19492886-19493004  22:19494910-19495040  22:19495290-19495387  22:19496054-19496214  22:19502273-19502410  22:19502489-19502571  22:19504051-19504168  22:19504340-19504416  22:19506367-19506431  22:19511523-19511778  22:19707126-19707221  22:19707330-19707415  22:19707639-19707761  22:19707844-19707977  22:19708073-19708189  22:19708292-19708392  22:19709164-19709259  22:19709346-19709480  22:19709761-19709862  22:19709951-19710007  22:19747167-19747200  22:19748647-19748803  22:19750765-19750865  22:19751679-19751849  22:19752482-19752636  22:19753281-19753348  22:19766744-19766930  22:19776233-19776483  22:19789525-19789739  22:19794183-19794280  22:19799809-19799970  22:19808122-19808246  22:19808752-19808878  22:19838691-19838889  22:19838891-19839089  22:19839091-19839289  22:19839291-19839489  22:19839491-19839784  22:19950051-19950066  22:19950128-19950338  22:19951090-19951282  22:19951692-19951822  22:19956060-19956259  22:19958752-19958858  22:19959410-19959494  22:19959881-19959934  22:19960261-19960350  22:19960449-19960559  22:19960643-19960840  22:19961167-19961316  22:19961636-19961762  22:19963209-19963280  22:19964229-19964246  22:19964939-19965109  22:19965482-19965598  22:19966421-19966603  22:19967267-19967465  22:19968935-19969260  22:19969457-19969614  22:20024322-20024377  22:20030879-20030966  22:20039989-20040088  22:20040961-20041074  22:20043466-20043536  22:20049054-20049206  22:20050862-20050965  22:20052066-20052185  22:20073488-20073686  22:20073688-20073886".split("  ")
  var ds : Array[Long] = Array()

  // The following are parameters needed for each sample.
  // They are initialised empty, and should be set for each sample.
  var obs : Array[Double] = Array()
  // Null since Objects cannot have variables undefined. Hacky.
  var emissions : Array[Array[BigDecimal]] = _        // [State][Target]
  var transitions : Array[Array[Array[BigDecimal]]] = _ // [Target][State][State]

  // TODO: Handle the differences between chromosomes. Currently results in a hack in line 85.
  private def calc_d_values() : Unit = {
    if (debug) println("Calculating d values")
    var t0 = System.nanoTime()
    val data = targets map { (a) => {
        a.split(":").drop(1)
      }
    }
    for (a <- data.indices) {
      data(a) = data(a)(0).split("-")
    }

    var numData = data.flatMap({case a: Array[String] => a}).map(_.toLong).grouped(2).toArray

    var diff = mutable.ListBuffer[Long]()
    for (i <- 0 to data.length - 2) {
      diff += (numData(i + 1)(0) - numData(i)(1) - 1) // Subtract additional 1 because we only want the distance between genes
    }
    ds = diff.toArray
    var t1 = System.nanoTime()
    println("Done Calculating d values")
    if (debug) println("Elapsed time: " + (t1 - t0)/1000000000.0 + "seconds")
  }

  private def calc_transition_probabilities() : Unit = {
    var f: BigDecimal = 0.0; var target: Int = 0
    var a: BigDecimal = 0.0; var b: BigDecimal = 0.0; var c: BigDecimal = 0.0; var d: BigDecimal = 0.0; var e: BigDecimal = 0.0
    transitions = Array.ofDim[BigDecimal](ds.length+1, 3, 3)
    if (debug) println("Calculating transitions")
    var t0 = System.nanoTime()
    for (i <- ds.indices) {

      // Hack to let this work for now; substitute Infinity for a huge number
      val power = Math.pow(Math.E, (-BigDecimal(ds(i)) / D).toDouble)
      f = if (power.isInfinity) BigDecimal(1000000000) // BigDecimal to fix underflow issues
          else BigDecimal(power)
      target = i+1; a = 1 - f; b = 1 - 2*p; c = f*q + a*b; d = a * p; e =f * (1-q) + d

      transitions(target)(1)(1) = b
      transitions(target)(1)(2) = p
      transitions(target)(1)(0) = p

      transitions(target)(0)(1) = c
      transitions(target)(0)(2) = d
      transitions(target)(0)(0) = e

      transitions(target)(2)(1) = c
      transitions(target)(2)(2) = e
      transitions(target)(2)(0) = d
    }
    var t1 = System.nanoTime()
    println("Done calculating transitions")
    if (debug) println("Elapsed time: " + (t1 - t0)/1000000.0 + " milliseconds")
  }

  private def calc_emission_probabilities() : Unit = {
    if (debug) println("  Calculating emissions ")
    var t0 = System.nanoTime()
    for (a <- obs.indices) {
      val cur = obs(a)
      emissions(0)(a) = BigDecimal(normDistDel.density(cur))
      emissions(1)(a) = BigDecimal(normDistDip.density(cur))
      emissions(2)(a) = BigDecimal(normDistDup.density(cur))
    }
    var t1 = System.nanoTime()
    println("  Done calculating emissions ")
    if (debug) println("  Elapsed time: " + (t1 - t0)/1000000.0 + " milliseconds")
  }

  private def clear_model() : Unit = {
    fwdCache.clear()
    bckCache.clear()
  }

  def calc_common_variables() : Unit = {
    println("Initialising model..")
    calc_d_values()
    calc_transition_probabilities()
    emissions = Array.ofDim[BigDecimal](3, ds.length+1)
  }

  def calc_probabilities_for_sample() : Unit = {
    clear_model()
    calc_emission_probabilities()
    calculate_Forward_Backward()
    println("  Done calculating probabilites for sample")
  }

  def main(args: Array[String]): Unit = {
  }

}
