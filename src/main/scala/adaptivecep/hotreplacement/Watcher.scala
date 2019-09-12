package adaptivecep.hotreplacement

import java.io.File
import java.nio.file.Paths

import adaptivecep.Loader
import adaptivecep.data.Events.{Init, Update}
import akka.actor.{Actor, ActorRef}

object Watcher extends Actor {
  val projectPath: String = Paths.get(getClass.getResource("").getPath).getParent.toString
  var files: Map[File, Long] = Map[File, Long]()
  var integrator: ActorRef = null

  def findFiles(folder: File): Unit = {
    if(folder.exists() && folder.isDirectory) {
      folder.listFiles().toList.foreach(f => {
        if(f.isDirectory) {
          findFiles(f)
        }else{
          if(!f.getName.contains("$"))
            files += (f -> f.lastModified())
        }
      })
    }
  }

  def updateFile(file: File): Boolean = {
    val clazz = Loader.update(file)
    if(clazz != null) {
      integrator ! Update(clazz)
      return true
    }
    return false
  }

  def watchFiles() = {
    while(true) {
      var update: File = null
      val iter = files.iterator
      while (iter.hasNext) {
        val elem = iter.next()
        val checkFile: File = new File(elem._1.toURI)
        if (checkFile.lastModified() != elem._2) {
          if(updateFile(checkFile)){
            update = checkFile
          }
        }
      }
      if (update != null) {
        files -= update
        files += (update -> update.lastModified())
      }
      Thread.sleep(2000)
    }
  }


  override def receive: Receive = {
    case Init =>
      findFiles(new File(projectPath))
      integrator = sender()
      watchFiles()
  }
}
