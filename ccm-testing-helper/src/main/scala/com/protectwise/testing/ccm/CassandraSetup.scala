package com.protectwise.testing.ccm

import java.io._
import java.util.jar.{JarEntry, JarOutputStream}

import com.protectwise.util.Configuration
import org.apache.commons.io.FileUtils

import scala.concurrent.duration._
import scala.concurrent._
import scala.collection.JavaConverters._

object CassandraSetup {
  import scala.concurrent.ExecutionContext.Implicits.global

  val markerFile = new File("cassandra.externally.managed.marker")

  lazy val config = Configuration.load().get[Configuration]("cassandra.ccm")

  lazy val clusters: Seq[CassandraCluster] = {
    val r = config.getConfigList("clusters").toSeq.flatten.map(CassandraCluster.clusterFromConfig)
    r.map(_.clusterName).groupBy(identity).filterNot(_._2.size == 1).map { case (n, c) =>
      throw new Exception(s"Duplicate cluster names found.  There are ${c.size} clusters defined using the name name '$n'.")
    }
    r
  }

  lazy val createCassandra = {
    println("Creating Cassandra Instances")
    val r = clusters.map { i => {
      val ccm = i.ccm
      new File(i.rootDir).mkdirs()

      println(s"  Creating ${i.clusterName}")
      ccm.create(i).!
    }}

    // Copy in additional jar files
    clusters.foreach { cluster =>
      cluster.datacenters.foreach { dc =>
        dc.nodes.filter(_.workload == "solr").foreach { n =>
          for (filename <- cluster.additionalJars ++ dc.additionalJars ++ n.additionalJars) {
            // We're sticking this in the tomcat/lib/ path for the node because it's a predictable node-specific location
            // which is also not inside the binary management path for CCM, but does get picked up by the classloader.
            val targetDir = s"${cluster.rootDir}/${cluster.clusterName}/${n.nodeName}/resources/tomcat/lib/"
            val src = new File(filename)
            if (src.isDirectory) {
              val dst = new File(targetDir + "[^a-zA-Z0-9_.-]+".r.replaceAllIn(filename, "_") + ".jar")

              println(s"    Copying $src/* -> $dst")
//              FileUtils.copyDirectory(src, dst, new FileFilter {
//                override def accept(pathname: File): Boolean = {
//                  println(s"      .../${pathname.getPath.drop(filename.length)} -> .../${pathname.getPath.drop(filename.length)}")
//                  true
//                }
//              }, true)
              JarHelper.createJarForPath(src, dst)
            } else {
              val dst = new File(targetDir + src.getName)
              println(s"    Copying $src -> $dst")
              FileUtils.copyFile(src, dst, true)
            }
          }
        }
      }
    }
  }

  lazy val startCassandra = {
    println("Starting Cassandra Instances")
    val r = clusters.map { i => Future {
      val ccm = i.ccm

      println(s"  Starting ${i.clusterName}")
      Await.result(ccm.startFuture(i), 90.seconds)

      // Return the ccm cqlsh command for convenience of the developer
      ccm.NodeCommand("cqlsh", i.datacenters.head.nodes.head.nodeName).toString
    }}
    val ccmCommands = Await.result(Future.sequence(r), Duration.Inf)
    println(s"""|
      |================================================================================
      | Cassandra clusters started, use the following commands to access cqlsh:
      |    ${ccmCommands.mkString("\n    ")}
      |================================================================================
      |""".stripMargin)
  }

  lazy val setupCqlSchema = {
    println("Setting CQL Schema")
    val r = clusters.map { i => Future {
      val ccm = i.ccm
      println(s"  Setting CQL Schema for ${i.clusterName}")
      i.cqlSchemaFiles.map { f =>
        ccm.setSchemaFile(i, f).!
      }
      val pscr = ccm.postSchemaCommands(i).!
      if (pscr != 0) System.err.println(s"Command failed with return code $pscr")
      // Return the ccm cqlsh command for convenience of the developer
      ccm.NodeCommand("cqlsh", i.datacenters.head.nodes.head.nodeName).toString
    }}
    val ccmCommands = Await.result(Future.sequence(r), Duration.Inf)
    println(
      s"""|
          |================================================================================
          | Cassandra clusters schema applied, use the following commands to access cqlsh:
          |    ${ccmCommands.mkString("\n    ")}
          |================================================================================
          |""".stripMargin)
  }

  lazy val teardownCassandra = {
    import scala.sys.process._

    println("Destroying Cassandra Instances")
    // Spit these out quickly, the clears do most of the work, we really want them to get started before the JVM exits.
    val r = clusters.map { i => Future {
      val ccm = i.ccm
      println(s"  Destroying ${i.clusterName}")
      ccm.clear(i).!
      ccm.remove(i).!
    }}
    Await.result(Future.sequence(r), Duration.Inf)
    Seq("sh", "-c", "ps ax | grep cassandra | grep java | grep 'Dcassandra.config.loader' | awk '{print $1}' | xargs kill -9").!
  }

  lazy val showAlreadyExistsMessage = {
    val ccmCommands = clusters.map(i=>i.ccm.NodeCommand("cqlsh", i.datacenters.head.nodes.head.nodeName).toString)
    println(s"""|
         |================================================================================
         | File ${markerFile.getName} exists, skipping Cassandra setup.
         | Use the following commands to access cqlsh:
         |    ${ccmCommands.mkString("\n    ")}
         |================================================================================
         |""".stripMargin)
    sys.addShutdownHook(println(s"""|
         |================================================================================
         | File ${markerFile.getName} exists, leaving Cassandra running.
         | Use the following commands to access cqlsh:
         |    ${ccmCommands.mkString("\n    ")}
         |================================================================================
         |""".stripMargin
    ))

  }

  def doAllSetup(): Unit = {
    println("Making sure Cassandra is all up and jazzy.")
    if (!markerFileExists) {
      sys.addShutdownHook(teardownCassandra)
      createCassandra
      startCassandra
      setupCqlSchema
    } else {
      showAlreadyExistsMessage
    }
  }

  def touchMarkerFile(): Unit = {
    markerFile.createNewFile()
  }
  def removeMarkerFile(): Unit = {
    markerFile.delete()
  }
  lazy val markerFileExists = markerFile.exists()

  def usage(): Unit = {
    println(
      """Usage: cassandra [startall|stopall|create|start|schema|stop|clear|remove]
        |
        |  startall - Implies create start schema
        |  stopall  - Implies stop clear remove
        |
        |  create   - Creates clusters in a stopped state with no schemas
        |  start    - Starts previously created clusters (clusters must
        |             previously have been created for this to succeed)
        |  schema   - Applies the schema to started clusters (clusters must
        |             already be started for this to succeed)
        |  stop     - Shuts down all clusters but preserves data, clusters can
        |             be started again later and will restore all their state
        |  clear    - Deletes all data on previously created clusters but does not
        |             delete the clusters.  They can be started again, but will not
        |             have any schema
        |             + implies stop
        |  remove   - Completely deletes previously created clusters and all data,
        |             system should now be left in a fully clean state
        |             + implies stop
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    if (args.size == 0) {
      usage()
      sys.exit(1)
    }

    for (operation <- args) {
      println(s"Beginning $operation")
      operation match {
        case "startall" =>
          createCassandra
          startCassandra
          setupCqlSchema
          touchMarkerFile()
        case "stopall" =>
          teardownCassandra
          removeMarkerFile()
        case "create" =>
          createCassandra
        case "start" =>
          startCassandra
        case "schema" =>
          setupCqlSchema
          touchMarkerFile()
        case "stop" =>
          clusters.map { i =>
            val ccm = i.ccm
            ccm.stop(i).!
          }
        case "clear" =>
          clusters.map { i =>
            val ccm = i.ccm
            ccm.clear(i).!
          }
          removeMarkerFile()
        case "remove" =>
          clusters.map { i =>
            val ccm = i.ccm
            ccm.remove(i).!
          }
          removeMarkerFile()
        case other =>
          println(s"Unknown command: $other\n")
          usage()
          sys.exit(2)
      }
    }
    sys.exit(0)
  }
}
