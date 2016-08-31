/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

import com.protectwise.testing.ccm.CassandraSetup._
import com.protectwise.util.Configuration
import com.typesafe.config.ConfigList

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

object CassandraCluster {
  val CCM = Configuration.load().get[String]("CCM", "tools/ccm/ccm")
  var globalNodeIdx = 0

  def clusterFromConfig(c: Configuration): CassandraCluster = {
    val dataCenterConfigs: Seq[Configuration] = c.getConfigList("datacenters").toSeq.flatten

    def addDataCentersToDse(dataCenterConfigs: Seq[Configuration], cluster: DseCassandra) = {
      cluster.copy(datacenters=dataCenterConfigs.map(dataCenterFromConfig(_, cluster)))
    }
    def addDataCentersToCommunity(dataCenterConfigs: Seq[Configuration], cluster: CommunityCassandra) = {
      cluster.copy(datacenters=dataCenterConfigs.map(dataCenterFromConfig(_, cluster)))
    }
    val postStartCommands: Seq[Seq[String]] = c.getList("postSchemaCommands").map(_.asScala.toList.map {
      case l: ConfigList => l.iterator().asScala.toList.map(_.unwrapped.toString)
      case _ => throw new Exception("postSchemaCommands must be an array of arrays")
    }) getOrElse Seq()

    c.get[String]("type") match {
      case "dse" =>
        addDataCentersToDse(dataCenterConfigs, DseCassandra(
          c.get[String]("rootDir"),
          c.get[String]("name"),
          c.get[String]("version"),
          c.get[String]("dseUsername"),
          c.get[String]("dsePassword"),
          null,
          c.getStringList("schemaFiles") getOrElse Seq(),
          c.getConfig("updateConf").map(updateConfRecordsFromConfig) getOrElse Seq(),
          c.getConfig("updateDseConf").map(updateConfRecordsFromConfig) getOrElse Seq(),
          c.getOpt[String]("existingBinariesDir"),
          postStartCommands,
          JarHelper.getJarsForClassNames(c.getStringList("additionalClassJars") getOrElse List())
        ))
      case "community" =>
        addDataCentersToCommunity(dataCenterConfigs, CommunityCassandra(
          c.get[String]("rootDir"),
          c.get[String]("name"),
          c.get[String]("version"),
          null,
          c.getStringList("schemaFiles") getOrElse Seq(),
          c.getConfig("updateConf").map(updateConfRecordsFromConfig) getOrElse Seq(),
          c.getOpt[String]("existingBinariesDir"),
          postStartCommands,
          JarHelper.getJarsForClassNames(c.getStringList("additionalClassJars") getOrElse List())
        ))
      case other => throw new Exception(s"Do not know how to build a cluster of type $other")
    }
  }

  /** Pulls a ccm updateconf compatible set of config key/val pairs from configuration
    * @param c
    * @return
    */
  def updateConfRecordsFromConfig(c: Configuration): Seq[String] = {
    c.subKeys.toSeq.map { k =>
      s"$k:${c.get[String](k)}"
    }
  }

  def dataCenterFromConfig(c: Configuration, cluster: CassandraCluster) = {
    val nodeConfigs = c.getConfigList("nodes").toSeq.flatten
    val dc = CassandraDC(
      cluster = cluster,
      dcName = c.get[String]("name"),
      workload = c.get[String]("workload"),
      nodes = null,
      additionalJars = JarHelper.getJarsForClassNames(c.getStringList("additionalClassJars") getOrElse List())
    )

    dc.copy(nodes = nodeConfigs.map(nodeFromConfig(_, cluster, dc, c.get[String]("workload"))))
  }

  def nodeFromConfig(c: Configuration, cluster: CassandraCluster, datacenter: CassandraDC, workload: String) = {
    CassandraNode(
      cluster = cluster,
      datacenter = datacenter,
      nodeName = c.get[String]("name"),
      jmxPort = c.get[Int]("jmxPort"),
      remoteDebugPort = c.get[Int]("remoteDebugPort"),
      addr = c.get[String]("address"),
      workload = workload,
      solrPort = c.getOpt[Int]("solrPort"),
      additionalJars = JarHelper.getJarsForClassNames(c.getStringList("additionalClassJars") getOrElse List())
    )
  }

}

trait CassandraCluster {
  def clusterName: String
  def version: String
  def options: Seq[String] = Seq()
  def installDir: Option[String]
  def datacenters: Seq[CassandraDC]
  def cqlSchemaFiles: Seq[String]

  def createOptions: Seq[String] = installDir.map(i => s"--install-dir=$i").toSeq

  def confUpdates: Seq[String]
  def dseConfUpdates: Seq[String]

  def isDse: Boolean
  def postSchemaCommands: Seq[Seq[String]] = Seq.empty
  def rootDir: String

  def additionalJars: Set[String]

  protected def optFlag(opt: Boolean, flag: String) = opt match {
    case true => Some(flag)
    case _ => None
  }

  def create()(implicit ccm: CCM) = ccm.create(this)
  def start()(implicit ccm: CCM) = ccm.start(this)

  lazy val ccm = CCM(CassandraCluster.CCM, Seq(s"--config-dir=${rootDir}"))

  def mapNodes[T](f: CassandraNode=>T): ListMap[CassandraNode, T] = {
    ListMap(datacenters.flatMap { dc =>
      dc.nodes.map(n => n -> f(n))
    }:_*)
  }
}
