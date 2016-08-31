/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import org.specs2.mutable.{After, Specification}

class CQLHelperSpec extends Specification {

  "CQLHelper" should {

    "provide a stringcontext to compose CQL statements" in {
      cql"foo" must beAnInstanceOf[CQLStatement]
    }

    "provide parameter binding" in {
      val bar=1
      val c = cql"foo $bar"
      c.cqlParts mustEqual Seq("foo ", "")
      c.rawParams mustEqual Seq(bar)
    }

    "handle the In() operator" in {
      val c = cql"foo ${In(Seq(1,2))} bar"
      c.cql mustEqual "foo ?,? bar"
      c.parameters mustEqual Seq(1,2)
      c.debugString(null) mustEqual "foo 1,2 bar;"
    }

    "handle the In() operator with just one value" in {
      val c = cql"foo ${In(Seq(1))} bar"
      c.cql mustEqual "foo ? bar"
      c.parameters mustEqual Seq(1)
    }

    "handle an empty In() operator" in {
      val c = cql"foo ${In(Seq())} bar"
      c.cql mustEqual "foo  bar"
      c.parameters mustEqual Seq()
    }

    "accept an Inline() text" in {
      val c = cql"foo ${Inline("x")} bar"
      c.cql mustEqual "foo x bar"
      c.parameters mustEqual Seq()
    }

    "handle a NoArg" in {
      val c = cql"foo $NoArg bar"
      c.cql mustEqual "foo  bar"
      c.parameters mustEqual Seq()
    }

    "handle .withValues on an In() operator" in {
      val a = cql"a ${} b ${} c ${} d"
      val b = a.withValues(1, In(Seq(2,3,4)), 5)
      b.cql mustEqual "a ? b ?,?,? c ? d"
      b.parameters mustEqual(Seq(1,2,3,4,5))
    }

  }

}
