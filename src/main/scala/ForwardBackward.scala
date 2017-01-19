
import scala.collection.mutable.{HashMap, ListBuffer, Map}
import org.apache.commons.math3.distribution.NormalDistribution
import Types._
/**
  * Created by davinchia on 1/19/17.
  */

object ForwardBackward {
  private val fwdCache: Map[(Int, State), Double] = new HashMap
  private val bckCache: Map[(Int, State), Double] = new HashMap

  // Default Forward-Backward parameters are empty and need to be assigned.
  var states : List[State] = List()
  var obs : List[Double] = List()
  var transitions : Map[(Int, State, State), Double] = Map()
  var emissions : Map[(String, Int), Double] = Map()

  var start : State => Probability = {
    case "Default" => 1.0
  }

  def forwardE(t: Int, j: State): Double = {
    val key = (t, j)
    if (fwdCache contains key) {
      fwdCache(key)
    } else {
      val result = if (t > 0) {
        Math.log((states map { i =>
          Math.exp(Math.log(forwardE(t - 1, i)) + Math.log(transitions(t, i, j)))
        } sum)) + Math.log(emissions(j, t))
      } else {
        Math.log(start(j)) + Math.log(emissions(j, 0))
      }
      fwdCache(key) = Math.exp(result)
      Math.exp(result)
    }
  }

  def backwardE(t: Int, i: State): Double = {
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

  def prob_state_from_t1_to_t2(t1: Int, t2: Int, state: State) : BigDecimal = {
    val fwd = forwardE(t1, state)
    val bck = backwardE(t2+1, state)
    println("fwd: " + fwd)
    println("bck: " + bck)
    //    println()

    var total = (states map { (s) => { forwardE(obs.length-1, s) * backwardE(obs.length, s) } } sum)
    println("total: " + total)

    val indiv = { (t1+1 to t2) map { (t) => { transitions(t, state, state) * emissions(state, t) } } }.reduceLeft(_*_) * fwd * bck

    //    for (t <- t1+1 to t2) {
    //      println("trans: " + transitions(t, state, state) )
    //      println("emiss: " + emissions(state, t) )
    //    }

    println("indiv: " + indiv)
    val prob = BigDecimal(indiv) / BigDecimal(total)
    println("prob: " + prob)
    prob
  }

  def phred_state_from_t1_to_t2(t1: Int, t2: Int, state: State) : Double = {
    -10 * Math.log10((BigDecimal(1) - prob_state_from_t1_to_t2(t1, t2, state)).toDouble)
  }

  def prob_state_from_t1_to_t2_inverse(t1: Int, t2: Int, state: State) : BigDecimal = {
    val fwd = forwardE(t1, state)
    val bck = backwardE(t2+1, state)
    println("fwd: " + fwd)
    println("bck: " + bck)
    //    println()

    var total = (states map { (s) => { forwardE(obs.length-1, s) * backwardE(obs.length, s) } } sum)
    println("total: " + total)

    val indiv = { (t1+1 to t2) map { (t) => { transitions(t, state, state) * emissions(state, t) } } }.reduceLeft(_*_) * fwd * bck

    //    for (t <- t1+1 to t2) {
    //      println("trans: " + transitions(t, state, state) )
    //      println("emiss: " + emissions(state, t) )
    //    }

    println("indiv: " + indiv)
    val prob = (BigDecimal(total)-BigDecimal(indiv)) / BigDecimal(total)
    prob
  }

  def phred_state_from_t1_to_t2_inverse(t1: Int, t2: Int, state: State) : Double = {
    val a = prob_state_from_t1_to_t2_inverse(t1, t2, state)
    println(a); println(BigDecimal(1) - a)
    -10 * Math.log10(a.toDouble)
  }

  def main(args: Array[String]): Unit = {
    // These are preset constant used to calculate probabilities
    val p = 0.00000001
    val t = 6
    val q = 1.0 / t
    val m = 3
    val D = BigDecimal(70000)

    // Process the targets to calculate f for each target
    val str = "22:16449025-16449223  22:16449225-16449423  22:16449425-16449804  22:17071768-17071966  22:17071968-17072166  22:17072168-17072366  22:17072368-17072566  22:17072568-17072766  22:17072768-17072966  22:17072968-17073166  22:17073168-17073440  22:17577952-17577976  22:17578688-17578833  22:17579666-17579777  22:17581246-17581371  22:17582886-17582933  22:17583030-17583192  22:17584385-17584467  22:17585617-17585700  22:17586481-17586492  22:17586744-17586841  22:17588617-17588658  22:17589198-17589396  22:17589398-17589596  22:17589598-17589796  22:17590198-17590396  22:17590398-17590710  22:17600282-17600480  22:17600882-17601080  22:17601082-17601091  22:17618912-17619247  22:17619441-17619628  22:17621950-17622123  22:17623988-17624021  22:17625915-17626007  22:17629339-17629450  22:17630433-17630635  22:17646099-17646134  22:17662374-17662466  22:17662711-17662912  22:17663495-17663651  22:17669230-17669337  22:17670833-17670922  22:17672574-17672700  22:17680439-17680468  22:17684454-17684663  22:17687962-17688180  22:17690247-17690567  22:18062711-18062730  22:18063864-18063925  22:18064124-18064179  22:18065330-18065419  22:18066183-18066300  22:18069903-18070067  22:18070692-18070845  22:18072356-18072431  22:18072862-18073002  22:18075440-18075502  22:18077296-18077382  22:18080961-18081054  22:18082793-18082861  22:18083859-18083947  22:18095578-18095644  22:18095978-18096086  22:18102227-18102292  22:18111369-18111401  22:18138479-18138598  22:18165981-18166087  22:18171753-18171908  22:18178907-18178976  22:18185010-18185152  22:18209444-18209642  22:18209644-18209842  22:18209844-18210042  22:18210044-18210300  22:18218346-18218357  22:18220784-18220995  22:18222115-18222254  22:18226570-18226779  22:18232871-18232940  22:18256377-18256455  22:18562641-18562780  22:18566204-18566498  22:18567879-18568024  22:18570739-18570841  22:18604247-18604468  22:18606924-18607071  22:18609122-18609320  22:18609322-18609520  22:18609522-18609801  22:18613611-18613903  22:18640432-18640587  22:18642940-18643035  22:18644558-18644702  22:18650023-18650101  22:18650658-18650803  22:18652612-18652706  22:18653521-18653687  22:18655918-18656048  22:18656560-18656609  22:18893889-18893997  22:18894079-18894238  22:18897686-18897785  22:18898402-18898541  22:18899054-18899202  22:18900689-18900875  22:18900952-18901039  22:18904404-18904501  22:18905830-18906004  22:18906965-18907110  22:18907220-18907311  22:18908856-18908936  22:18909839-18909917  22:18910331-18910446  22:18910628-18910692  22:18912565-18912713  22:18913201-18913235  22:18918504-18918711  22:19026379-19026634  22:19028572-19028807  22:19029321-19029472  22:19035954-19036156  22:19044500-19044675  22:19050715-19050791  22:19052362-19052580  22:19055614-19055738  22:19076882-19077003  22:19118914-19119112  22:19119114-19119312  22:19119314-19119512  22:19119514-19119712  22:19119714-19119989  22:19121710-19121988  22:19122574-19122688  22:19124837-19124945  22:19125729-19125830  22:19126673-19126805  22:19127126-19127242  22:19127369-19127537  22:19130052-19130146  22:19130240-19130407  22:19132020-19132153  22:19163644-19163757  22:19163934-19164007  22:19164092-19164206  22:19164360-19164463  22:19164634-19164717  22:19165241-19165378  22:19318964-19319079  22:19338882-19338969  22:19340880-19341042  22:19341520-19341641  22:19343284-19343388  22:19343753-19343811  22:19344414-19344574  22:19346860-19347007  22:19348761-19348864  22:19349251-19349454  22:19363155-19363315  22:19365393-19365589  22:19371144-19371228  22:19373045-19373259  22:19375235-19375339  22:19376007-19376077  22:19379625-19379737  22:19381866-19382032  22:19384311-19384470  22:19385516-19385610  22:19393310-19393403  22:19394708-19394797  22:19396007-19396116  22:19398239-19398301  22:19418963-19418999  22:19420789-19420871  22:19422260-19422417  22:19423162-19423485  22:19438193-19438267  22:19442273-19442353  22:19443204-19443291  22:19444110-19444157  22:19444376-19444396  22:19445594-19445662  22:19452725-19452797  22:19455397-19455526  22:19459211-19459331  22:19462591-19462623  22:19462994-19463125  22:19467492-19467542  22:19467681-19467740  22:19468477-19468568  22:19470214-19470350  22:19471386-19471528  22:19481850-19481905  22:19483504-19483552  22:19484909-19484970  22:19486624-19486674  22:19492886-19493004  22:19494910-19495040  22:19495290-19495387  22:19496054-19496214  22:19502273-19502410  22:19502489-19502571  22:19504051-19504168  22:19504340-19504416  22:19506367-19506431  22:19511523-19511778  22:19707126-19707221  22:19707330-19707415  22:19707639-19707761  22:19707844-19707977  22:19708073-19708189  22:19708292-19708392  22:19709164-19709259  22:19709346-19709480  22:19709761-19709862  22:19709951-19710007  22:19747167-19747200  22:19748647-19748803  22:19750765-19750865  22:19751679-19751849  22:19752482-19752636  22:19753281-19753348  22:19766744-19766930  22:19776233-19776483  22:19789525-19789739  22:19794183-19794280  22:19799809-19799970  22:19808122-19808246  22:19808752-19808878  22:19838691-19838889  22:19838891-19839089  22:19839091-19839289  22:19839291-19839489  22:19839491-19839784  22:19950051-19950066  22:19950128-19950338  22:19951090-19951282  22:19951692-19951822  22:19956060-19956259  22:19958752-19958858  22:19959410-19959494  22:19959881-19959934  22:19960261-19960350  22:19960449-19960559  22:19960643-19960840  22:19961167-19961316  22:19961636-19961762  22:19963209-19963280  22:19964229-19964246  22:19964939-19965109  22:19965482-19965598  22:19966421-19966603  22:19967267-19967465  22:19968935-19969260  22:19969457-19969614  22:20024322-20024377  22:20030879-20030966  22:20039989-20040088  22:20040961-20041074  22:20043466-20043536  22:20049054-20049206  22:20050862-20050965  22:20052066-20052185  22:20073488-20073686  22:20073688-20073886"
    val data = str.split("  ") map { (a) => {
      a.split(":").drop(1)
    }
    }
    for (a <- 0 to data.length - 1) {
      data(a) = data(a)(0).split("-")
    }
    var diff = new ListBuffer[Int]()
    for (i <- 0 to data.length - 2) {
      diff += (data(i + 1)(0).toInt - data(i)(1).toInt - 1) // Subtract additional 1 because we only want the distance between genes
    }
    val ds = diff.toList

    states = List("Diploid", "Duplication", "Deletion")
    start = {
      case "Diploid" => 1.0
      case "Duplication" => 0.00000001
      case "Deletion" => 0.00000001
    }

    // Table 2
    var f: Double = 0.0

    for (i <- 0 to ds.length - 1) {
      f = Math.pow(Math.E, (-BigDecimal(ds(i)) / D).toDouble) // BigDecimal to fix underflow issues

      transitions += ((i + 1, "Diploid", "Diploid") -> (1 - 2 * p))
      transitions += ((i + 1, "Diploid", "Duplication") -> p)
      transitions += ((i + 1, "Diploid", "Deletion") -> p)

      transitions += ((i + 1, "Deletion", "Diploid") -> (f * q + (1 - f) * (1 - 2 * p)))
      transitions += ((i + 1, "Deletion", "Duplication") -> ((1 - f) * p))
      transitions += ((i + 1, "Deletion", "Deletion") -> (f * (1 - q) + (1 - f) * p))

      transitions += ((i + 1, "Duplication", "Diploid") -> (f * q + (1 - f) * (1 - 2 * p)))
      transitions += ((i + 1, "Duplication", "Duplication") -> (f * (1 - q) + (1 - f) * p))
      transitions += ((i + 1, "Duplication", "Deletion") -> ((1 - f) * p))
    }

    val normDistDip = new NormalDistribution(0.0, 1.0)
    val normDistDel = new NormalDistribution(-m, 1.0)
    val normDistDup = new NormalDistribution(m, 1.0)

    obs = List(-0.41299542138535994, -0.17490180390834065, -0.1886903954362933, 5.705381609827105, 3.84614534088253, 3.8822764225899706, 4.361319603035853, 5.377935125975705, 3.309127305045965, 2.567932700586726, 2.9226448417596953, -1.425176049639377, 0.7703639470620648, 0.7986864410584029, -0.7881716626570838, -1.0481716629159221, -0.39715144462685303, -1.008721233926663, -0.5285845715168708, 0.40194282031593137, -0.005845512715160396, 0.208503351194057, 0.21984502887548563, 0.11489591016961839, -0.518333693466908, -0.2036526448687854, -0.11694852471165292, -0.24398196212350712, -0.059914669949907545, -0.015416000483052224, 0.21165736464844373, -0.2794477890318955, -0.39660178204027463, -0.6115961304433222, -1.1540928016525038, -0.5943826085744839, -1.6943581134841887, 0.12827478281922852, -0.5626624808544248, -0.8164588598309396, -1.7246958537789816, 0.060621251266108234, 0.07985642137894848, 0.22390403989474425, -1.168704712879237, -0.574945816383214, -0.8972192585488876, -0.45783073121133183, 0.4073359090611979, 0.00784571688484403, 0.07850695363108275, -0.7610024889391638, 1.0915486765719293, 0.4194711608330125, -0.8041094898177343, 0.11630560663154148, -0.29750518922165625, -0.19439963714106767, -0.8290397178015066, -0.5745709114948853, 0.14830697728580666, -2.9494060966841182, -0.5021190051422628, -0.20464438503733404, 1.2588320522294403, -0.688552799821285, -0.8709245607339339, -0.8189779750945482, -0.22835287828848555, -1.0646459192859572, -1.9023941357461422, -1.0129519712227575, -0.5046605268399372, -0.5857827079645878, -0.41914829126167386, -0.9305482060973715, -0.16057297094513856, -0.21718607907621693, 0.08862339370875247, -0.0031263251987375773, -0.26335034082901204, -0.4751824147848261, -0.5919169498671119, 0.6527808019684539, -0.5532552496435065, -0.04379652378302663, -0.1343369175146906, -0.19558491178572826, -0.5902703821189086, -0.4641153729514392, -0.15963630983289825, -0.9485500846805567, -0.38366815399993354, 0.5973522652832822, 0.063259425929353, -0.43465062421969464, 0.34648300482269456, -0.28582449391923276, 1.161698597389376, -0.8579869063124709, 0.14123914193885742, 0.35311371791574636, 0.14728689477016557, 0.5985422027720173, 0.973100546421345, 0.23901197920283085, -0.3615029950211245, 0.3926730419436433, 0.25405032655517495, 0.7297252172286538, 1.0412074333158492, 1.664554471803238, 1.300928661218219, 0.9456642718656647, 0.28011338603212926, 0.8993966962725745, 0.7150309485543244, -0.28417876458864744, 0.19431403069022096, 0.730676334255033, -0.001096990726276442, -0.7373391518063453, 0.42120093897503036, 0.33781241155356495, 0.05933788055583659, 0.18551495643259636, -0.15126191825819657, -0.16865467978009535, -0.3184989140537152, -0.27599577600135694, 0.18384892838022318, 0.10318429784967939, 2.2142703665946297, 0.6642735264532265, -0.09655864003419246, 0.1487470315649175, 0.1868524018790649, -0.023300899115258678, -0.2935699270540008, -0.014035958433090308, 0.2575661223927808, 0.18121429051661855, -0.22561911677558436, 0.5328376600866541, 0.010002545998571666, -0.2594758920112018, -0.25791822846095397, -0.12205958672509015, -0.5631274286372445, -0.3851560663657924, -0.17383704671887715, -0.36422508804252995, -1.2328419371434955, 0.1794611668733193, 1.9078983943460055, -0.10517479810411362, -0.5928902817412423, 0.06383635011518592, -0.36087220929516645, -0.25967025554323714, -1.9731620817965567, -0.40010311002577026, 1.207277012517435, -0.42092893724050306, -2.1441744747219892E-4, -0.23606105893084026, -0.7080811760198379, 1.5243734258329065, -0.8533568287418831, -0.5159752603450781, -0.6475832788289629, 0.012552371787854478, -0.17387953200435496, -0.3092917449755063, -1.748909822222431, -0.8742219789720919, -0.5698871883108405, -2.3485960311243526, -0.7244597619475204, -0.5289280073235504, -0.39810896873775353, 0.22574228886395378, -0.39542098677600457, -1.4389167301449364, -1.507744873361603, -1.3677469673243479, -0.6271333555838458, 0.07076693335277956, 0.20526849360260166, -0.9633904391867886, -1.2469316282481293, -0.19300383283528125, 0.8998162717321841, -0.2545282267038634, -0.5091507669887143, 0.9288516834402663, -0.9601489592209054, 0.9163233413007548, 0.8487863321010698, 0.4655924909179534, -0.9421189749725903, -0.9260246547395681, -0.02047453830872115, 0.08933316801852416, -0.2768044776885456, 0.061940088764151545, -0.2999485576293211, -0.2937267329685674, -0.4334509365269478, 0.21104513366905975, 0.13565577178262964, 1.4803256553725137, -0.30038726508514896, -0.23009051256683408, 0.10721422963406455, 0.11413674127238557, 0.5111860732193523, 0.17982703053147023, 0.4516633214178752, -0.5431511359205725, 0.13596128201601151, -0.24455973203281023, 0.507891522934411, -0.26136659568445614, 0.07212492973297516, -0.6604362222696908, -0.2502811833529639, -0.04729091898068624, 0.17072749333038326, 1.6972255018239955, 0.3825513019326401, 0.017810010777895933, -0.3040713641603809, 0.20404857606490104, -0.3641945737316692, -0.27143200917710486, -0.4448847766866019, -0.16465727504099167, -0.5087883005967095, -0.2226792035785778, -0.062328811004172334, 0.0026452126570638565, 0.4775471770450577, -0.003190292207256803, -0.09025303597150054, -0.3020898086504384, 0.52180771432573, 2.0823992286625708, -0.12100062959092478, 0.09418898100586202, 0.1746273146436808, 0.7135083468314715, -0.32932143493506716, -0.12321987785147442, 0.5920279611807209, -0.7830359220088903, 0.5641543067858067, -0.6837014811691406, 0.4505585619735497, 0.9816869523843937, 0.2060506608106767, 0.23039353574833665, 0.36661490529255486, 0.019569368421358906, -0.1718602944149187)

    var emission_probability: Map[(String, Int), Double] = Map()
    // Generate emission probabilities for each sample
    for (a <- 0 to obs.length - 1) {
      val cur = obs(a)
      emission_probability += (("Diploid", a) -> normDistDip.density(cur))
      emission_probability += (("Duplication", a) -> normDistDup.density(cur))
      emission_probability += (("Deletion", a) -> normDistDel.density(cur))
    }
    emissions = emission_probability
  }
}
