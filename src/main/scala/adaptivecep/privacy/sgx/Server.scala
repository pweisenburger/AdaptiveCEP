package adaptivecep.privacy.sgx

import java.rmi.registry.{LocateRegistry, Registry}
import java.rmi.server.UnicastRemoteObject

object Server {
  var registry: Registry = null
//  val lock: Object = new Object
  def main(args: Array[String]): Unit = {

    try {
      /// arguments order [policyfile] [host address] [port]
      /// notice that scala io does not work on top of sgx-lkl
      /// so it is important to pass these values as arguments when running the server
      if (args.length > 0) {
        System.setProperty("java.security.policy", args(0))
      }
      else {
        System.setProperty("java.security.policy", "file:///c:/test.policy")
      }

      if(args.length > 1) {
        System.setProperty("java.rmi.server.hostname",args(1))
      }
      else{
        println("Enter server host name (IP address) <enter to ignore>: ")
        val address = scala.io.StdIn.readLine()
        if(!address.isEmpty){
          System.setProperty("java.rmi.server.hostname",address)
        }
      }
      var port = 0
      if(args.length > 2) {
        port = Integer.parseInt(args(2))
      }
      else {
        print("Enter Port number: ")
        val portInput = scala.io.StdIn.readLine()
        port = Integer.parseInt(portInput)
      }

      if (System.getSecurityManager == null)
        System.setSecurityManager(new SecurityManager)

      val name: String = "eventProcessor"
//
//      def bindObject[A <: java.rmi.Remote](name: String, myRemoteObj: A): Unit = {
//
//      }

      val myRemoteObj = new EventProcessorServiceImpl



      val stub = UnicastRemoteObject.exportObject(myRemoteObj, 0).asInstanceOf[EventProcessorServer]
      registry = LocateRegistry.createRegistry(port)
      registry.rebind(name, stub)

//      bindObject(name, new EventProcessorServiceImpl)

      println("Server ready.. listening on port " + port)
//      lock.wait()
    } catch {
      case e: java.rmi.server.ExportException =>
        serverAlreadyRunningWarning()
      case e: java.rmi.RemoteException =>
        println("remote exception " + e.getMessage)
      case e: Exception =>
        println("Something went wrong " + e.getMessage)
    }

    def serverAlreadyRunningWarning() {
      println("Unable to bind port, perhaps another server is already running?")
    }


  }

}
