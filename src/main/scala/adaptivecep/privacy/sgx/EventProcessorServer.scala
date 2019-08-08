package adaptivecep.privacy.sgx

import java.rmi.{Remote, RemoteException}

import adaptivecep.data.Events.Event

trait EventProcessorServer extends Remote{

  /**
    * the service should decrypt the event, apply the predicate and return the result
    *
    * @param cond
    * @param input the input event for the condition
    * @throws java.rmi.RemoteException
    * @return whether the event correspond to the condition closure
    */
  @throws(classOf[RemoteException])
  def applyPredicate(cond: Event => Boolean, input: Event) : Boolean


}
