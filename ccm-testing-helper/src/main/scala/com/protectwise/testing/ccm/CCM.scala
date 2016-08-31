/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.ProcessBuilder

case class CCM(exec: String = "ccm", defaultOpts: Seq[String] = Seq()) {
  trait Command {
    def command: ProcessBuilder
    def continueOnFailure: Boolean
    def ++(other: Commands) = {
      other.copy(this +: other.commands:_*)
    }
    def ++(other: ClusterCommand) = {
      Commands(this, other)
    }
    def :+(other: ClusterCommand) = {
      Commands(this, other)
    }
  }
  case class Commands(commands: Command*) {
    def ! : Int = {
      var failed = false
      val returns = for {
        cmd <- commands
        if !failed
      } yield {
        println(s"    => $cmd")
        val exitCode = cmd.command.!
        if (exitCode != 0 && !cmd.continueOnFailure) failed = true
        exitCode
      }
      returns.lastOption getOrElse 0
    }
    def copy(cmds: Command*) = Commands(cmds:_*)
    def ++(other: Commands) = {
      copy(commands ++ other.commands:_*)
    }
    def :+(other: Command) = {
      copy(commands :+ other:_*)
    }
    def ++(other: Command) = {
      copy(commands :+ other:_*)
    }
  }

  protected def cliEscapeArgs(args: String*): String = {
    args.map { arg =>
      if ("^[a-zA-Z0-9=/_-]+$".r.findFirstIn(arg).isDefined) arg
      else s"${'"'}${arg.replace("\"", "\\\"")}${'"'}"
    }.mkString(" ")
  }

  case class ClusterCommand(op: String, cmdFlags: String = "", options: Iterable[Any] = Seq(), continueOnFailure: Boolean = false) extends Command {
    override def toString() = s"$exec $op ${cliEscapeArgs(opts:_*)} $cmdFlags"
    def opts = defaultOpts ++ options.map(_.toString)
    def command: ProcessBuilder = {
      import sys.process._
      stringSeqToProcess(Seq(exec, op) ++ opts :+ cmdFlags)
    }
  }
  case class NodeCommand(op: String, node: String, options: Iterable[Any] = Seq(), continueOnFailure: Boolean = false) extends Command {
    override def toString() = s"$exec $node $op ${cliEscapeArgs(opts:_*)}"
    def opts = defaultOpts ++ options.map(_.toString)
    def command: ProcessBuilder = {
      import sys.process._
      stringSeqToProcess(Seq(exec, node, op) ++ opts)
    }
  }

  object SystemCommand {
    def apply(args: String*) = new SystemCommand(args)
  }
  case class SystemCommand(args: Seq[String], continueOnFailure: Boolean = false) extends Command {
    override def toString = cliEscapeArgs(args:_*)
    override def command: ProcessBuilder = {
      import sys.process._
      stringSeqToProcess(args.map(_.toString))
    }
    def continueOnFailure(cof: Boolean = true): SystemCommand = copy(continueOnFailure = cof)
  }

  def create(i: CassandraCluster) = {
    val opts = i.createOptions ++ Seq(s"-v", i.version, "-b")

    // These have to happen after updateconf and updatedseconf because those will OVERWRITE this change.
    val solrCommands = i.datacenters.map { dc =>
      dc.nodes.map { node =>
        node.workload match {
          case "solr" if node.solrPort.isDefined =>
            val catalinaConf = s"${i.rootDir}/${i.clusterName}/${node.nodeName}/resources/tomcat/conf/catalina.properties"
            Commands(
              //              SystemCommand("sleep", "1"),
              SystemCommand("sed", "-i", "-e", s"s/http.port=.*/http.port=${node.solrPort.get}/", catalinaConf)
            )
          case _ => Commands()
        }
      }
    }.reduce(_ ++ _).reduce(_ ++ _)

    ClusterCommand("create", i.clusterName, opts) ++
    i.datacenters.map(addDc(_, i)).reduce(_ ++ _) ++
    // CCM gets this wrong at least in the current version, all nodes come up in one big DC rather than several sub-DC's
    ClusterCommand("updateconf", "endpoint_snitch:PropertyFileSnitch") ++
    Commands(i.confUpdates.map(ClusterCommand("updateconf", _)):_*) ++
    Commands(i.dseConfUpdates.map(ClusterCommand("updatedseconf", _)):_*) ++
    solrCommands

  }

  def addNode(node: CassandraNode, dc: CassandraDC, cluster: CassandraCluster): Commands = {
    val dseOpts = if (cluster.isDse) Some("--dse") else None

    Commands(
      ClusterCommand("add", node.nodeName, dseOpts ++ Seq(
        "--seeds",
        "-i", node.addr,
        "-j", node.jmxPort,
        "-r", node.remoteDebugPort,
        s"--data-center=${node.dcName}"
      )),
      NodeCommand("setworkload", node.nodeName, Seq(node.workload))
    )
  }

  def addDc(dc: CassandraDC, cluster: CassandraCluster) =
    dc.nodes.map(addNode(_, dc, cluster)).reduce(_ ++ _)

  def remove(i: CassandraCluster) = stop(i) ++ ClusterCommand("remove", i.clusterName)

  def start(i: CassandraCluster) = switch(i) ++ Commands(
    ClusterCommand("start", i.clusterName, Seq("-v", "--wait-for-binary-proto"))
  )

  def startFuture(i: CassandraCluster)(implicit ec: ExecutionContext) = {
    // Cardinal sin, should use akka scheduler or something, but don't want to introduce a dependency
    // This whole thing is also full of a lot of blocking system calls, and this isn't exactly a feature
    // meant for scalability.  So take that good coding practices!
    val startFuture: Future[Int] = Future.firstCompletedOf( Seq(
      Future { scala.concurrent.blocking {
        val r = start(i).!
        println(s"EXIT CODE of start command: $r")
        r
      } },
      Future { scala.concurrent.blocking { Thread.sleep(5000) }; throw new Exception("Timeout waiting for start") }
    ))


    def retry[T](times: Int)(f: =>Option[T]): T = {
      val r = f
      if (r.isDefined) r.get
      else if (times > 0) retry(times - 1)(f)
      else throw new Exception("Failed")
    }

    val nodes = i.datacenters.flatMap(_.nodes)

    val testNodes = Future.traverse(nodes) { node => Future {
      retry[Unit](30) {
        val r = Commands(NodeCommand("cqlsh", node.nodeName, Seq("-e", "SELECT peer FROM system.peers LIMIT 1"))).!
        if (r == 0) Some(Unit)
        else {
          println(s"EXIT CODE $r, node is not online yet, sleeping for 1 second")
          scala.concurrent.blocking { Thread.sleep(1000) }
          None
        }
      }
    }}

    val p: Promise[Unit] = Promise()

    startFuture andThen { case _ =>
      // We want to start testing nodes, REGARDLESS of whether startFuture completed successfully
      // This is because CCM BLOCKS FOREVER starting some versions of Cassandra.  startFuture
      // will Fail if it takes more than 5 seconds for ccm start to come back.  Then we'll
      // start testing to see if we can query all the nodes, which will run for up to another 10
      // seconds.  If THAT fails, then the whole thing fails.  And we have left a dirty started
      // operation potentially outstanding.
      p.completeWith(testNodes.map(_ => Unit))
    }

    p.future
  }

  def clear(i: CassandraCluster) = switch(i) ++ Commands(ClusterCommand("clear", i.clusterName))

  def stop(i: CassandraCluster) = switch(i) ++ Commands(ClusterCommand("stop", i.clusterName))

  def switch(i: CassandraCluster) = Commands(ClusterCommand("switch", i.clusterName))
  def switch(i: CassandraDC) = Commands(ClusterCommand("switch", i.clusterName))
  def switch(i: CassandraNode) = Commands(ClusterCommand("switch", i.clusterName))

  def reset(i: CassandraCluster) = Commands(ClusterCommand("switch", i.clusterName))

  def postSchemaCommands(i: CassandraCluster): Commands = Commands(i.postSchemaCommands.map(cmd => SystemCommand(cmd:_*)):_*)

  def setSchemaFile(i: CassandraCluster, filename: String): Commands = setSchemaFile(i.datacenters.head, filename)

  def setSchemaFile(i: CassandraDC, filename: String): Commands = setSchemaFile(i.nodes.head, filename)

  def setSchemaFile(i: CassandraNode, filename: String): Commands = switch(i) ++ Commands(NodeCommand("cqlsh", i.nodeName, Seq("-f", filename, "-v")))
}
