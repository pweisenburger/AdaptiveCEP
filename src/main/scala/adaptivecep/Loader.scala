package adaptivecep

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileNotFoundException, InputStream}
import java.net.{URL, URLClassLoader}
import java.nio.file.Paths

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Loader extends ClassLoader {
  val projectPath = Paths.get(getClass.getResource("").getPath).getParent.toString
  var actualBinaryName = ""
  var loadedClasses: ListBuffer[Class[_]] = ListBuffer[Class[_]]()
  var classLoaderMap: Map[Class[_], ClassLoader] = Map[Class[_], ClassLoader]()


  def load(pFile: File): Class[_] = {
    val file: File = new File(pFile.toURI)
    val newCL = new URLClassLoader(Array[URL](file.toURI.toURL), this) {
      def loadClass(classData: Array[Byte], name: String): Class[_] = {
        val clazz = defineClass(name, classData, 0, classData.length)

        if (clazz != null) {
          if (clazz.getPackage == null) {
            definePackage(name.replaceAll("\\.\\w+$", ""),
              null, null, null,
              null, null, null, null)
          }
          resolveClass(clazz)
        }
        clazz
      }
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        Option(findLoadedClass(name)).orElse({Try(findClass(name)).toOption}).getOrElse(super.loadClass(name, resolve))
      }
    }

    var clazz:Class[_] = null

    try {
      val classData = readByteData(new FileInputStream(new File(file.toURI)))
      clazz = newCL.loadClass(classData, actualBinaryName)

      loadedClasses += clazz
      classLoaderMap += (clazz -> newCL)

      //Load [..]$$anonfun$receive$1.class to avoid LinkageError
      val receiveClass = new File(file.getAbsolutePath.stripSuffix(".class") + "$$anonfun$receive$1.class")
      val receiveClassData = readByteData(new FileInputStream(receiveClass))
      val reloadReceive = newCL.loadClass(receiveClassData, actualBinaryName + "$$anonfun$receive$1")
    }catch{
      case f: FileNotFoundException => println("File not found")
      case e: Exception => println(e)
    }
    clazz
  }

  def updateRemote(classData: Array[Byte], name: String): Class[_] = {
    val pathToFile = projectPath + name.stripPrefix("adaptivecep").replace(".", "/").concat(".class")

    val oldClazz:Class[_] = findClass(name)
    if(loadedClasses.contains(oldClazz)) {
      loadedClasses -= oldClazz
    }
    if(classLoaderMap.contains(oldClazz)){
      classLoaderMap -= oldClazz
      System.gc()
    }

    val newCL = new URLClassLoader(Array[URL](new File(pathToFile).toURI.toURL), this){
      def loadClass(classData: Array[Byte], name: String): Class[_] = {
        val clazz = defineClass(name, classData, 0, classData.length)

        if (clazz != null) {
          if (clazz.getPackage == null) {
            definePackage(name.replaceAll("\\.\\w+$", ""),
              null, null, null,
              null, null, null, null)
          }
          resolveClass(clazz)
        }
        clazz
      }
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        Option(findLoadedClass(name)).orElse({Try(findClass(name)).toOption}).getOrElse(super.loadClass(name, resolve))
      }
    }

    var clazz: Class[_] = null
    try {
      clazz = newCL.loadClass(classData, name)
    }catch{
      case classFormatError: ClassFormatError =>
        println(classFormatError)
        classFormatError.printStackTrace()
    }
    if(clazz != null) {
      loadedClasses += clazz
      classLoaderMap += (clazz -> newCL)
    }
    clazz
  }

  def update(pFile: File): Class[_] = {
    val file = new File(pFile.toURI)
    actualBinaryName = file.getAbsolutePath.split(projectPath)(1)
      .replaceFirst("/", "").stripSuffix(".class")
      .replace("/", ".")

    val oldClazz:Class[_] = findClass(actualBinaryName)
    if(loadedClasses.contains(oldClazz)) {
      loadedClasses -= oldClazz
    }
    if(classLoaderMap.contains(oldClazz)){
      classLoaderMap -= oldClazz
      System.gc()
    }
    load(file)
  }

  def load(name: String): Class[_] = {
    var fileToLoad: File = null
    val directory = new File(projectPath)
    directory.listFiles().toList.foreach(dir => {
      val f = findFiles(dir)
      if(f.getName.contains(name)) {
        fileToLoad = f
      }
    })
    if(fileToLoad != null) {
      return load(fileToLoad)
    } else {
      loadClass(name)
    }
  }

  def findFiles(file: File): File = {
    if(file.isDirectory){
      file.listFiles().toList.foreach(f => findFiles(f))
    }
    return file
  }

  def readByteData(in: InputStream): Array[Byte] = {
    var baos: ByteArrayOutputStream = null
    var data: Array[Byte] = null
    var n: Int = 0

    data = new Array[Byte](8192)
    baos = new ByteArrayOutputStream

    n = in.read(data, 0, 8192)
    while (n > -1) {
      baos.write(data, 0, n)
      n = in.read(data, 0, 8192)
    }

    val classData = baos.toByteArray

    classData
  }

  def getOrLoad(name: String): Class[_] = {
    if(!loadedClasses.isEmpty) {
      loadedClasses.foreach(c => {
        if(c.getName.contains(name)) {
          return c
        }
      })
    }
    return load(name)
  }

  override def findClass(name: String): Class[_] = {
    var clazz: Class[_] = null
    if(name.contains("adaptivecep")) {
      clazz = getOrLoad(name)
    } else {
      clazz = super.findClass(name)
    }
    clazz
  }
}
