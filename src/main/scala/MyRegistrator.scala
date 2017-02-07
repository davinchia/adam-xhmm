import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by joey on 1/29/17.
  */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[scala.Array[Int]])
    kryo.register(classOf[scala.Array[String]])
    kryo.register(classOf[scala.Array[Double]])
    kryo.register(classOf[scala.Array[BigDecimal]])
    kryo.register(classOf[scala.math.BigDecimal])
    kryo.register(classOf[java.math.BigDecimal])
    kryo.register(classOf[java.math.MathContext])
    kryo.register(classOf[java.math.RoundingMode])
    kryo.register(classOf[scala.Array[scala.Array[Int]] ])
    kryo.register(classOf[scala.Array[scala.Array[BigDecimal]] ])
    kryo.register(classOf[scala.Array[scala.Array[Double]] ])
    kryo.register(classOf[scala.Array[scala.Array[scala.Array[BigDecimal]]] ])

    kryo.register(Class.forName("breeze.linalg.DenseVector$mcD$sp"))
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseVector])
    kryo.register(classOf[scala.Array[org.apache.spark.mllib.linalg.Vector]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    kryo.register(classOf[Sample])
    kryo.register(classOf[scala.Array[Sample]])
  }
}
