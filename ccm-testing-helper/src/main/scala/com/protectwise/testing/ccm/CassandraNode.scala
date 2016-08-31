/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

case class CassandraNode(
  cluster: CassandraCluster,
  datacenter: CassandraDC,
  nodeName: String,
  addr: String,
  jmxPort: Int,
  remoteDebugPort: Int,
  workload: String,
  solrPort: Option[Int] = None,
  additionalJars: Set[String] = Set()
) {

  def clusterName: String = cluster.clusterName
  def dcName: String = datacenter.dcName

  lazy val jmxClient = jajmx.JMX(addr, jmxPort)

  def jmxCommand[A](signature: String, operation: String, args: Any*): Option[A] = {
    val bean = jmxClient(signature)
    bean.call[A](operation, args:_*)
  }

  def disableAutoCompaction(keyspace: String, tables: String*): Unit = {
    jmxCommand("org.apache.cassandra.db:type=StorageService", "disableAutoCompaction", keyspace, tables.toArray)
  }

  def flush(keyspace: String, tables: String*): Unit = {
    jmxCommand("org.apache.cassandra.db:type=StorageService", "forceKeyspaceFlush", keyspace, tables.toArray)
  }
}
