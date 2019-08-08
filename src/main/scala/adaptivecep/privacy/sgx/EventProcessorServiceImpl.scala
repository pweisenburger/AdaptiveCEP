package adaptivecep.privacy.sgx
import adaptivecep.data.Events._

class EventProcessorServiceImpl extends EventProcessorServer {
  /**
    * the service should decrypt the event, apply the predicate and return the result
    * @param cond
    * @param input the input event for the condition
    * @throws java.rmi.RemoteException
    * @return whether the event correspond to the condition closure
    */
  override def applyPredicate(cond: Event => Boolean, input: Event): Boolean =
    (cond,input) match {
      case ( f: (Event1 => Boolean) , e: Event1) => f(e)
      case ( f: (Event2 => Boolean) , e: Event2) => f(e)
      case ( f: (Event3 => Boolean) , e: Event3) => f(e)
      case ( f: (Event4 => Boolean) , e: Event4) => f(e)
      case ( f: (Event5 => Boolean) , e: Event5) => f(e)
      case ( f: (Event6 => Boolean) , e: Event6) => f(e)
      case _ => sys.error("unexpected type!")
    }








}
