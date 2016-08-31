/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.testing.ccm

case class CassandraDC(
  cluster: CassandraCluster,
  dcName: String,
  workload: String,
  nodes: Seq[CassandraNode],
  additionalJars: Set[String] = Set()
  ) {

  def clusterName: String = cluster.clusterName

}
