package com.protectwise.testing.ccm

import java.io.{FileOutputStream, FileInputStream, BufferedInputStream, File}
import java.util.jar.{JarEntry, JarOutputStream}

object JarHelper {

  /**
   * Returns the set of jars housing the identified classes.
   *
   * It should be noted that this may be a naked directory for classes
   * defined outside of jar files
   * @param classes full class names
   * @return
   */
  def getJarsForClassNames(classes: Iterable[String]): Set[String] = {
    getJarsForClasses(classes.map(getClass.getClassLoader.loadClass))
  }

  /**
   * Returns the set of jars housing the identified classes.
   *
   * It should be noted that this may be a naked directory for classes
   * defined outside of jar files
   * @param classes classes to be located
   * @return
   */
  def getJarsForClasses(classes: Iterable[Class[_]]): Set[String] = {
    classes.map(_.getProtectionDomain.getCodeSource.getLocation.getFile).toSet
  }

  /**
   * Creates a jar file from the source directory to the target filename.
   * @param path source directory
   * @param targetJar target jar filename
   * @return
   */
  def createJarForPath(path: File, targetJar: File): File = {
    def add(source: File, target: JarOutputStream): Unit = {
      var in: BufferedInputStream = null
      try {
        var name = source.getPath.replace("\\", "/").drop(path.getPath.length()+1)
        if (source.isDirectory && !name.isEmpty && !name.endsWith("/")) name += "/"
        println(s"      $name")
        if (source.isDirectory) {
          if (!name.isEmpty) {
            val entry = new JarEntry(name)
            entry.setTime(source.lastModified())
            target.putNextEntry(entry)
            target.closeEntry()
          }
          source.listFiles.foreach(add(_, target))
          return
        }

        val entry = new JarEntry(name)
        entry.setTime(source.lastModified())
        target.putNextEntry(entry)
        in = new BufferedInputStream(new FileInputStream(source))

        val buffer = Array.ofDim[Byte](1024)
        var count = 0
        while (count != -1) {
          count = in.read(buffer)
          if (count >= 0) target.write(buffer, 0, count)
        }
        target.closeEntry()
      } finally {
        if (in != null) in.close()
      }
    }

    //    val manifest = new java.util.jar.Manifest()
    val target = new JarOutputStream(new FileOutputStream(targetJar))
    add(path, target)
    target.close()

    targetJar
  }
}
