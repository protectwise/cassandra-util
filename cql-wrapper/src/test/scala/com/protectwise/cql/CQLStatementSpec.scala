/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import java.nio.ByteBuffer

import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, ListSet}

class CQLStatementSpec extends Specification with DataTables {

  implicit lazy val sess: CQLSession = null//client.session

  "CQLStatement" should {

    "add (+) two statements together to create a single statement" in {
      // To maintain parameter alignment, cql"A $p1 B" + cql"C $p2 D" must be the same as cql"A $p1 BC $p2 D"
      // This is CQLStatement(Seq("A ", " B"), Seq(p1)) + CQLStatement(Seq("C ", " D"), Seq(p2))
      // The end result of the statement portion must take the tail of the first concatenated with the head of the second
      // replacing the tail of the first, and dropping the head of the second, then both lists concatenated together.
      // So the final result is CQLStatement(Seq("A ", " BC ", " D"), Seq(p1, p2))

      val a = CQLStatement(Seq("A", "B"), Seq[AnyRef](1: java.lang.Integer), Map.empty)
      val b = CQLStatement(Seq("C", "D"), Seq[AnyRef](2: java.lang.Integer), Map.empty)
      val expected = CQLStatement(Seq("A", "BC", "D"), Seq[AnyRef](1: java.lang.Integer, 2: java.lang.Integer), Map.empty)
      val actual = a + b

      "has right cql text" ==> (actual.cqlParts mustEqual expected.cqlParts)
      "has right parameters" ==> (actual.rawParams mustEqual expected.rawParams)
    }

    "add (+) two SingleStatementBatches together to create a single single-statement batch" in {
      import com.protectwise.cql.CQLHelper
      val one = 1
      val two = 2
      val actual = cql"A $one B" + cql"C $two D" + cql"E"
      val expected = cql"A $one BC $two DE"

      actual mustEqual expected
    }

    "accept new values" in {
      cql"a ${} b".withValues(1) mustEqual cql"a ${1} b"
    }

    "provide a debug cql representation" in {
      "Statement" || "Expected" |
      cql"int=${1}" !! "int=1;" |
      cql"string=${"2"}" !! "string='2';" |
      cql"string=${"a'b"}" !! "string='a''b';" |
      cql"string=${"a''b"}" !! "string='a''''b';" |
      cql"some[int]=${Some(1)}" !! "some[int]=1;" |
      cql"none=${None}" !! "none=null;" |
      cql"some[string]=${Some("1")}" !! "some[string]='1';" |
      cql"some[string]=${Some("a'b")}" !! "some[string]='a''b';" |
      cql"some[string]=${Some("a''b")}" !! "some[string]='a''''b';" |
      cql"set[int]=${ListSet(1,2)}" !! "set[int]={1,2};" |
      cql"set[string]=${ListSet("a")}" !! "set[string]={'a'};" |
      cql"set[string]=${ListSet("a'b")}" !! "set[string]={'a''b'};" |
      cql"list[int]=${List(1,2)}" !! "list[int]=[1,2];" |
      cql"list[string]=${List("a","b")}" !! "list[string]=['a','b'];" |
      cql"list[string]=${List("a'b","c''d")}" !! "list[string]=['a''b','c''''d'];" |
      cql"map[string,string]=${Map("a'b"->"c''d")}" !! "map[string,string]={'a''b':'c''''d'};" |
      cql"map[int,int]=${ListMap(1->2,3->4)}" !! "map[int,int]={1:2,3:4};" |> {
        (statement, expected) => {
          statement.debugString(null) mustEqual expected
        }
      }
    }


    "convert Some(primitive)" in {
      cql"${Some(1)}".rawParams mustEqual Seq(1)
    }

    "convert Some(string)" in {
      cql"${Some("a")}".rawParams mustEqual Seq("a")
    }

    "convert None to null" in {
      cql"$None".rawParams mustEqual Seq(null)
    }

    "convert Scala list to Java list" in {
      cql"${List(1)}".rawParams.head must beAnInstanceOf[java.util.List[_]]
    }

    "convert Scala Set to Java set" in {
      cql"${Set(1)}".rawParams.head must beAnInstanceOf[java.util.Set[_]]
    }

    "convert Scala Map to Java Map" in {
      cql"${Map(1->2)}".rawParams.head must beAnInstanceOf[java.util.Map[_,_]]
    }

    "convert Some(List) to Java list" in {
      cql"${Some(List(1))}".rawParams.head must beAnInstanceOf[java.util.List[_]]
    }

    "convert Some(Set) to Java set" in {
      cql"${Some(Set(1))}".rawParams.head must beAnInstanceOf[java.util.Set[_]]
    }

    "convert Some(Map) to Java Map" in {
      cql"${Some(Map(1->2))}".rawParams.head must beAnInstanceOf[java.util.Map[_,_]]
    }

    "convert Set[Array[Byte]] to Set[ByteBuffer]" in {
      val c = cql"${Set(Array[Byte](0,1,2,3))}"
      val h = c.rawParams.head
      h must beAnInstanceOf[java.util.Set[_]]
      h.asInstanceOf[java.util.Set[_]].asScala.head must beAnInstanceOf[ByteBuffer]
    }

    "convert List[Array[Byte]] to List[ByteBuffer]" in {
      val c = cql"${List(Array[Byte](0,1,2,3))}"
      val h = c.rawParams.head
      h must beAnInstanceOf[java.util.List[_]]
      h.asInstanceOf[java.util.List[_]].asScala.head must beAnInstanceOf[ByteBuffer]
    }

    "convert Map[Array[Byte], Array[Byte]] to Map[ByteBuffer, ByteBuffer]" in {
      val c = cql"${Map(Array[Byte](4,5,6,7) -> Array[Byte](0,1,2,3))}"
      val h = c.rawParams.head
      h must beAnInstanceOf[java.util.Map[_, _]]
      h.asInstanceOf[java.util.Map[_, _]].asScala.head._1 must beAnInstanceOf[ByteBuffer]
      h.asInstanceOf[java.util.Map[_, _]].asScala.head._2 must beAnInstanceOf[ByteBuffer]
    }

    "offer basic stripmargin option" in {
      val actual =
        cql"""foo
           |bar
           |baz
           |""".stripMargin
      val expected =
        """foo
          |bar
          |baz
          |;""".stripMargin
      actual.debugString mustEqual expected
    }

    "offer stripmargin with a different char" in {
      val actual =
        cql"""foo
           ~bar
           ~baz
           ~""".stripMargin('~')
      val expected =
        """foo
          ~bar
          ~baz
          ~;""".stripMargin('~')
      actual.debugString mustEqual expected
    }

    "offer parameterized stripmargin option" in {
      val actual =
        cql"""foo ${1}
           |bar
           |${2} baz
           |${3} zop ${4}
           |zom
           |""".stripMargin
      val expected =
        """foo 1
          |bar
          |2 baz
          |3 zop 4
          |zom
          |;""".stripMargin
      val expectedCQL =
        """foo ?
          |bar
          |? baz
          |? zop ?
          |zom
          |""".stripMargin
      actual.debugString mustEqual expected
      actual.cql mustEqual expectedCQL
    }

    "handle named parameters" in {
      cql"FOO ${'pos1} BAR ${'pos2}".withValues('pos1->1, 'pos2->2).parameters mustEqual Seq(1, 2)
    }

    "handle named parameters in the wrong order" in {
      cql"FOO ${'pos1} BAR ${'pos2}".withValues('pos2->2, 'pos1->1).parameters mustEqual Seq(1, 2)
    }

    "handle named parameters when concatenating statements" in {
      (cql"FOO ${'pos1}" + cql" BAR ${'pos2}").withValues('pos2->2, 'pos1->1).parameters mustEqual Seq(1, 2)
    }

    "handle named parameters when concatenating a string" in {
      (cql"FOO ${'pos1}" + "foo" + cql" BAR ${'pos2}").withValues('pos2->2, 'pos1->1).parameters mustEqual Seq(1, 2)
    }

    "handle a mix of positional and named parameters" in {
      (cql"FOO ${'pos1}" + "foo" + cql" BAR ${}").withValues('pos1->1, 2).parameters mustEqual Seq(1, 2)
    }

    "handle a mix of positional and named parameters out of order" in {
      (cql"FOO ${'pos1}" + "foo" + cql" BAR ${}").withValues(2, 'pos1->1).parameters mustEqual Seq(1, 2)
    }

    "accept a named Inline() because why not?" in {
      val st = (cql"FOO ${'pos1}" + "foo" + cql" BAR ${'pos2}").withValues('pos2 -> Inline("foo"), 'pos1->1)

      st.parameters mustEqual Seq(1)
    }

    "accept a named In() because why not?" in {
      val st = (cql"FOO ${'pos1}" + "foo" + cql" BAR ${'pos2}").withValues('pos2 -> In(Seq("foo", "bar")), 'pos1->1)
      st.parameters mustEqual Seq(1, "foo", "bar")
      st.cql mustEqual "FOO ?foo BAR ?,?"
    }

    "do keyval as named parameter" in {
      val st = cql"UPDATE foo SET ${'foo}".withValues('foo -> Fields(Seq("foo"->1, "bar"->2)))
      st.cql mustEqual "UPDATE foo SET foo=?,bar=?"
      st.parameters mustEqual Seq(1,2)
    }

    "accept keyval pairs for dynamic statement construction" in {
      val args = Seq("foo"->1, "bar"->2)
      val st = cql"UPDATE foo SET ${Fields(args)} WHERE x"
      st.cql mustEqual "UPDATE foo SET foo=?,bar=? WHERE x"
      st.parameters mustEqual Seq(1,2)
    }

    "handle nonEmpty constructor for keyval pairs" in {
      val args = Seq("foo"->1, "bar"->2, "baz"->None, "zop"->null)
      val st = cql"UPDATE foo SET ${NonEmptyFields(args)} WHERE x"
      st.cql mustEqual "UPDATE foo SET foo=?,bar=? WHERE x"
      st.parameters mustEqual Seq(1,2)
    }

  }

}
