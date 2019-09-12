package adaptivecep.simulation

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter, PrintWriter}

import adaptivecep.data.Events.Init
import akka.actor.Actor

class UpdateGeneratorLocal extends Actor{
  var start: Long = 0

  def simulateUpdates(amount: Int) = {
    var counter = 0
    // add path to .class file
    val classPath = ""
    // add path to file for results
    val fos = new FileOutputStream(new File(""))
    val bw = new BufferedWriter(new OutputStreamWriter(fos))

    while (counter < amount) {
      val file = new File(classPath)
      file.setLastModified(System.currentTimeMillis)
      start = file.lastModified()
      Thread.sleep(5000)
      val difference = UpdateGeneratorLocal.finish - start
      bw.write(difference.toString)
      bw.newLine()

      counter = counter +1
    }
    bw.flush()
    bw.close
  }

  def receive(): Receive = {
    case Init => simulateUpdates(1000)
  }
}

object UpdateGeneratorLocal {
  private var finish: Long = 0

  def setFinish(time: Long) = {
    finish = time
  }

  def getFinish(): Long ={
    finish
  }
}
