/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql.exceptions

case class StatementPrepareException(msg: String, cause: Throwable) extends Exception(msg, cause)
