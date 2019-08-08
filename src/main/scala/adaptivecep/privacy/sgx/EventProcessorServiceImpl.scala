package adaptivecep.privacy.sgx
import adaptivecep.data.Events

class EventProcessorServiceImpl extends EventProcessorServer {
  /**
    * the service should decrypt the event, apply the predicate and return the result
    * @param cond
    * @param input the input event for the condition
    * @throws java.rmi.RemoteException
    * @return whether the event correspond to the condition closure
    */
  override def applyPredicate(cond: Events.Event => Boolean, input: Events.Event): Boolean =
    cond(input)
}
