package biggi.util

import akka.actor.ActorSystem

/**
 * @author dirk
 *          Date: 10/9/13
 *          Time: 2:29 PM
 */
object BiggiActorSystem {
    val instance = ActorSystem("biggi")
}
