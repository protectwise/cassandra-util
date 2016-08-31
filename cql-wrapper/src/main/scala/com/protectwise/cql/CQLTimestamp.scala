/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import com.datastax.driver.core.{SimpleStatement, Statement, BatchStatement}

case object CQLNoTimestamp extends CQLTimestamp(None) {
}

object CQLTimestamp {
  def apply(ts: Long) = new CQLTimestamp(Some(ts))

}

class CQLTimestamp(val timestamp: Option[Long]) {
  def batchTimestampStatement: String = timestamp map { v => s" USING TIMESTAMP ? " } getOrElse ""

  def apply(batch: Statement): Statement = {
    timestamp foreach { ts => batch.setDefaultTimestamp(ts) }
    batch
  }

  def apply(batch: BatchStatement): BatchStatement = {
    timestamp foreach { ts => batch.setDefaultTimestamp(ts) }
    batch
  }

  def apply(batch: SimpleStatement): SimpleStatement = {
    timestamp foreach { ts => batch.setDefaultTimestamp(ts) }
    batch
  }

  override def toString: String = timestamp map { v => s" USING TIMESTAMP $v " } getOrElse ""

  def isDefined: Boolean = timestamp.isDefined

  def isEmpty: Boolean = timestamp.isEmpty
}
