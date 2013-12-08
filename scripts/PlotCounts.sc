/**
 * @author dirk
 *          Date: 12/8/13
 *          Time: 1:14 PM
 */
import breeze.plot._
import breeze.linalg._

val f = Figure()
val p = f.subplot(0)
val g = breeze.stats.distributions.Gaussian(0,1)
p += hist(g.sample(100000),100)
p.title = "A normal distribution"

