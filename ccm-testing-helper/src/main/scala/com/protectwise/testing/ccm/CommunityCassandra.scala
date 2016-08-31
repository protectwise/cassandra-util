/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

case class CommunityCassandra(
  rootDir: String,
  clusterName: String = "localcassandra",
  version: String = "2.0.11",
  datacenters: Seq[CassandraDC],
  cqlSchemaFiles: Seq[String] = Seq(),
  confUpdates: Seq[String] = Seq(),
  installDir: Option[String] = None,
  override val postSchemaCommands: Seq[Seq[String]] = Seq(),
  additionalJars: Set[String] = Set()
) extends CassandraCluster {

  // Doesn't apply to community cassandra
  override final def dseConfUpdates: Seq[String] = Seq()

  override final def isDse: Boolean = false
}
