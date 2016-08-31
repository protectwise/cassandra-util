/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

case class DseCassandra(
  rootDir: String,
  clusterName: String,
  version: String,
  dseUsername: String,
  dsePassword: String,
  datacenters: Seq[CassandraDC],
  cqlSchemaFiles: Seq[String] = Seq(),
  confUpdates: Seq[String] = Seq(),
  override val dseConfUpdates: Seq[String],
  installDir: Option[String] = None,
  override val postSchemaCommands: Seq[Seq[String]] = Seq(),
  additionalJars: Set[String] = Set()
) extends CassandraCluster {


  override def createOptions: Seq[String] = super.createOptions ++
    Seq("--dse", s"--dse-username=$dseUsername", s"--dse-password=$dsePassword")

//  def withCassandraDc(nodes: Int, name: String = "") = withDc(nodes, "cassandra", name)
//
//  def withSearchDc(nodes: Int, name: String = "") = withDc(nodes, "search", name)
//
//  def withDc(nodes: Int, workload: String, name: String = "") = {
//    val dcName = name match {
//      case "" => s"$workload-dc"
//      case n => n
//    }
//    copy(datacenters = datacenters :+ {
//      var dcNodeIdx = 0
//      CassandraDC(clusterName, dcName, workload, Seq.fill(nodes) {
//        CassandraCluster.globalNodeIdx += 1
//        dcNodeIdx += 1
//        CassandraNode(
//          clusterName = clusterName,
//          dcName = dcName,
//          nodeName = s"$workload-$dcNodeIdx",
//          addr = s"127.0.0.${CassandraCluster.globalNodeIdx}",
//          jmxPort = 7100 + CassandraCluster.globalNodeIdx,
//          remoteDebugPort = 2100 + CassandraCluster.globalNodeIdx,
//          workload = workload
//        )
//      })
//    })
//  }
  override final def isDse: Boolean = true

}
