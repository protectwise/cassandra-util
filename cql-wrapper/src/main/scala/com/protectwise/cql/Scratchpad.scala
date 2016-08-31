///*
// * Copyright 2014 ProtectWise, Inc.  All rights reserved
// * Unauthorized copying of this file by any means is strictly prohibited.
// */
//package com.protectwise.cql
//
//import com.datastax.driver.core.{Host, ResultSet}
//
//import scala.collection.immutable.IndexedSeq
//import scala.concurrent.{Await, ExecutionContext, Future}
//import scala.concurrent.duration._
//import com.protectwise.cql._
//
//class Scratchpad {
//  import com.protectwise.cql.CQLHelper
//  com.protectwise.cql.In
////  implicit val cassandra = CassandraClient("default").session
////  import scala.concurrent.ExecutionContext.Implicits.global
////
////  val a = 1
////  val b = "b"
////
////  s"foo $a"
////
////  val s1 = cql"INSERT INTO foo(a,b) VALUES ($a, $b)".withTimestamp(System.currentTimeMillis())
////  val s2 = cql"INSERT INTO bar(a,b) VALUES ($a, $b)"
////  val s3 = cql"INSERT INTO baz(a,b) VALUES ($a, $b)"
////
////  val batch: CQLBatch = s1 ++ s2 ++ s3
////
////  val preparedBatch = batch.prepareAll()
////
////  val result: Future[ResultSet] = preparedBatch.execute()
////
////  def updateCMS(name: String, data: Seq[Seq[Long]])(implicit ec: ExecutionContext) = {
////    val batch: CQLBatch = (for {
////      (wRow, dIdx) <- data.zipWithIndex
////      (value, wIdx) <- wRow.zipWithIndex
////    } yield {
////      cql"UPDATE cms SET value = value + $value WHERE name=$name AND d=$dIdx AND w=$wIdx"
////    }).foldLeft(CQLBatch())(_ ++ _)
////    batch.prepareAll().execute()
////  }
////
////  cql"INSERT INTO FOO (a,b,c) VALUES (?,?,?)".execute()
////
////  cql"DROP TABLE scratchpad.inserttest IF EXISTS".execute()
////  cql"""CREATE TABLE scratchpad.inserttest (
////       |  partition int,
////       |  cluster int,
////       |  value int,
////       |  PRIMARY KEY ((partition), cluster)
////       |)
////     """.stripMargin.execute()
//
//
//
//
//
//  import scala.concurrent.ExecutionContext.Implicits.global
//  import scala.concurrent.{Await, ExecutionContext, Future}
//  import scala.concurrent.duration._
//  import com.protectwise.cql._
//
//  implicit val cassandra = CassandraClient("default").session
//
//  val it: Iterator[IndexedSeq[Int]] = (0 to 10000).grouped(100)
//  for {
//    ii <- it
//  } {
//    val statements: IndexedSeq[CQLStatement] = (for (i <- ii) yield { Seq(
//      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (1, 1, $i) USING TIMESTAMP 1""".prepare(),
//      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (1, 5, $i) USING TIMESTAMP ${(1000000000 - i).toLong}""".prepare()
//      //      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (1, 2, $i) USING TIMESTAMP 1""".prepare(),
//      //      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (1, 3, $i) USING TIMESTAMP 1""".prepare(),
//      //      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (2, 1, $i) USING TIMESTAMP 1""".prepare(),
//      //      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (3, 1, $i) USING TIMESTAMP 1""".prepare(),
//      //      cql"""INSERT INTO scratchpad.inserttest (partition, cluster, value) VALUES (4, 1, $i) USING TIMESTAMP 1""".prepare()
//    )}).flatten
//
//    val batch: CQLUnloggedBatch = CQLBatch(statements)
//    val debug = batch.groupByFirstReplica.mapValues { vv =>
//      vv.statements.map{ s => s.debugString }.mkString("\n")
//    }
//    print(s"$debug ...")
//    val start = System.currentTimeMillis()
//    Await.result(batch.execute(), Duration.Inf)
//    println(f" ${System.currentTimeMillis() - start}%,d ms")
//  }
//}
