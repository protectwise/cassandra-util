/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.tracing

import scala.concurrent.Future
import scala.util.Try

case class CQLTrace[D, R](data: D, startedAt: Long, endedAt: Long, result: Try[R])
