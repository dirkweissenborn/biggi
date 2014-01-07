/**
 * @author dirk
 *          Date: 12/8/13
 *          Time: 1:14 PM
 */
import breeze.plot._

val f = Figure()
val p = f.subplot(0)
val roc = List[(Double,Double)]((1,1),(3,2.5),(2,2))
p += plot(roc.map(_._1),roc.map(_._2))
p += plot(List(0,1),List(0,1))

