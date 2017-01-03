/*
 * Copyright 2016 ProtectWise, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.protectwise.cassandra.compaction

import java.io.{File, FilenameFilter}
import java.nio.file.{FileSystems, NoSuchFileException, NotDirectoryException, StandardWatchEventKinds}

import com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy
import com.protectwise.cql._
import com.protectwise.logging.Logging
import com.protectwise.testing.ccm.{CassandraCluster, CassandraDC, CassandraNode, CassandraSetup}
import org.specs2.execute.Result
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.mutable.{BeforeAfter, Specification, SpecificationLike}
import org.specs2.specification.{Example, Fragments, Step}
import org.specs2.time.NoTimeConversions

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.sys.process.ProcessLogger

trait DeletingCompactionStrategySpecHelper extends Specification with Logging with NoTimeConversions with BeforeAfter {
  step({

    implicit lazy val session = client.session

    // Cleanup compaction strategies - this is to help with things like sstable2json which aren't going to understand
    // our deleting compaction strategy and will fail to read keyspace metadata necessary to deserialize this data.
    session.getCluster.getMetadata.getKeyspaces.asScala.foreach { ks =>
      ks.getTables.asScala.foreach { table =>
        val compaction = table.getOptions.getCompaction.asScala
        if (compaction("class") == classOf[DeletingCompactionStrategy].getName) {
          println(s"REVERTING compaction strategy for ${ks.getName}.${table.getName}.  This is probably leftover config from a previous run.")
          alterCompactionStrategy(ks.getName, table.getName)
        }
      }
    }

  })


  override def map(fs: => Fragments): Fragments = {
    super.map(
      step(debug(s"Before ${getClass.getSimpleName}")) ^
        fs.map {
          case f: Example =>
            val bodyProxy: () => Result = () => {
              print(s"  ${f.desc.withMarkdown}... ")
              val t = System.currentTimeMillis()
              val r = f.body()
              println(f"${(System.currentTimeMillis() - t).toFloat / 1000}%,.03f s")
              r
            }
            f.copy(f.desc, bodyProxy)
          case f => f
        } ^
        step(debug(s"After ${getClass.getSimpleName}"))
    )
  }

  lazy val onlyOnce = CassandraSetup.doAllSetup()
  lazy val client = CassandraClient("default")

  override def after: Any = {}

  override def before: Any = {
    onlyOnce
  }

  def abRangeData(ks: String, table: String, aRange: Range, bRange: Range): Iterable[CQLStatement] = {
    for {
      a <- aRange
      b <- bRange
    } yield {
      cql"INSERT INTO ${Inline(ks)}.${Inline(table)} (a,b,c,d) VALUES ($a, $b, ${100000+a}, ${100000+b})".prepare()
    }
  }

  def exec(cmd: String): Int = {
    exec(cmd.split(' ').toSeq)
  }

  def exec(cmd: Seq[String]): Int = {
    import scala.sys.process._

    val p = Process(cmd)

    def pl = ProcessLogger(logger.info(_))
    p.lines(pl)

    p.run().exitValue()
  }

  case class FileDifferences(node: CassandraNode, origFile: File, origSize: Long, afterSize: Long)

  def getDataDir(c: CassandraCluster, d: CassandraDC, n: CassandraNode): File = {
    val dataDir = new File(s"${c.rootDir}/${n.clusterName}/${n.nodeName}/data0/")

    if (!dataDir.exists() || !dataDir.isDirectory) {
      throw new Exception(s"Data directory can't be found for node ${n.clusterName}/${n.dcName}/${
        n.nodeName
      }.  Expected it to be at $dataDir")
    }
    dataDir
  }

  def getKsDataDir(c: CassandraCluster, d: CassandraDC, n: CassandraNode, ks: String) = {
    val ksDir = new File(getDataDir(c,d,n).getPath + s"/$ks/")
    if (!ksDir.exists() || !ksDir.isDirectory) {
      throw new Exception(
        s"Data directory can't be found for keyspace $ks under node ${n.clusterName}/${n.dcName}/${
          n.nodeName
        }.  Expected it to be at $ksDir"
      )
    }
    ksDir
  }

  def getTableDataDir(c: CassandraCluster, d: CassandraDC, n: CassandraNode, ks: String, table: String) = {
    val ksDir = getKsDataDir(c,d,n,ks)
    val candidates: Array[String] = ksDir.list(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.equals(table) || name.startsWith(s"$table-")
      }
    })
    if (candidates.isEmpty) {
      throw new Exception(
        s"Data directory can't be found for table $ks.$table under node ${n.clusterName}/${n.dcName}/${n.nodeName}"
      )
    } else if (candidates.length == 1) {
      new File(ksDir.getPath + "/" + candidates.head)
    } else {
      throw new Exception(
        s"Multiple candidate data directories found for table $ks.$table under node ${n.clusterName}/${n.dcName}/${n.nodeName}:\n${candidates.mkString("\n")}"
      )
    }
  }

  /**
    * Watches for a target file to be deleted/renamed, and returns the set of new files that exist in the target directory
    *
    * @param targetFile
    * @param knownFiles
    * @param filter
    * @param retries
    * @return
    */
  def watchForFileToRename(targetFile: File, knownFiles: Set[File], filter: FilenameFilter, retries: Int = 5): Set[File] = {
//    println(s"WATCH: $targetFile")
    if (targetFile.exists()) {
      import sys.process._
      val fs = FileSystems.getDefault
      val path = fs.getPath(targetFile.getParentFile.getPath)
      val ws = fs.newWatchService()
      try {
        path.register(ws, StandardWatchEventKinds.ENTRY_DELETE)
        // Blocks until the file delete is detected
        val event = scala.concurrent.blocking(ws.take())
      } catch {
        case e: NoSuchFileException =>
        // This will happen if compaction completed before we got to this point, which is pretty frequent actually
        case e: NotDirectoryException if retries > 0 =>
          // This happens with alarming frequency.  We know the path exists because we used File to find it,
          // and the path is the data directory for the node, which we've already flushed data out to
          // and which would exist even if we had never written data.
          logger.warn(s"Caught NotDirectoryException on ${path}, retrying ${retries-1} more times")
          val cmd = "ls -lah " + targetFile.getPath
          val out: String = cmd.!!
          println(cmd)
          println(out)
          Thread.sleep(200l)
          return watchForFileToRename(targetFile, knownFiles, filter, retries - 1)
      } finally {
        ws.close()
      }
    }


    knownFiles.foldLeft(targetFile.getParentFile.listFiles(filter).toSet) { case (acc, file) =>
      acc - file
    }
  }

  val baselineCompaction = Map(
    "class" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
    "min_threshold" -> "4",
    "max_threshold" -> "32"
  )

  def alterCompactionStrategy(ks: String, table: String, compaction: Map[String, String]=baselineCompaction)
                             (implicit session: CQLSession): Unit = {
    def escape(v: String) = v.replace("'", "''")
    val statements = Seq(
      cql"""ALTER TABLE ${Inline(s"$ks.$table")} WITH
            |compaction = {
            |  ${Inline(compaction.map{ case (k,v) => s"'${escape(k)}': '${escape(v)}'" }.mkString(",\n  "))}
            |}""".stripMargin
    )
//    statements.foreach(println)

    statements.foreach { st =>
      Await.result(st.execute(), 100.seconds)
    }

  }

  /**
    * Sets up a table to a vanilla state
    *
    * @param ks
    * @param table
    */
  def setupTestTable(
                      ks: String,
                      table: String,
                      dataStatements: TraversableOnce[CQLStatement],
                      compaction: Map[String, String] = baselineCompaction,
                      noInitialScrub: Boolean = false
                    )(implicit session: CQLSession): Unit = {

    val name = s"$ks.$table"

    if (!noInitialScrub) {
      // Setup
      Seq(
        cql"TRUNCATE ${Inline(name)}"
      ).foreach(st => Await.result(st.execute(), 100.seconds))

      alterCompactionStrategy(ks, table, compaction)

      CassandraSetup.clusters.foreach { c =>
        c.mapNodes { n =>
          n.disableAutoCompaction(ks, table)
        }
      }
    }

    val statements = dataStatements.toSeq
    Await.result(CQLBatch(statements).execute(), 100.seconds)

    // Flush to sstables
    flushTable(ks, table)
  }

  def flushTable(ks: String, table: String): Unit = {
    CassandraSetup.clusters.foreach { c =>
      c.mapNodes { n =>
        n.flush(ks, table)
      }
    }

  }

  val dataFileFilter = new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = name.endsWith("-Data.db")
  }

  def getDataFiles(c: CassandraCluster, d: CassandraDC, n: CassandraNode, ks: String, table: String): Map[File, Long] = {
    n.flush(ks, table)
    val dataDir = getTableDataDir(c,d,n,ks,table)

    dataDir.listFiles(dataFileFilter).map(f => f -> f.length()).toMap
  }

  def compactAllFiles(ks: String, table: String, groupSize: Int=1): Seq[FileDifferences] = {
    // Trigger user defined compaction on individual files
    val fileDifferences: Seq[FileDifferences] = CassandraSetup.clusters.flatMap { c =>
      c.datacenters.flatMap { d =>
        d.nodes.flatMap { n: CassandraNode =>
          val filesAndInitialSizes: Map[File, Long] = getDataFiles(c, d, n, ks, table)

          // Somewhere to hold the new files that show up as a result of compaction so we only consider
          // them new for the first pass
          var transientFiles = Set.empty[File]

          filesAndInitialSizes.grouped(groupSize).flatMap { fais =>
//            n.jmxCommand("org.apache.cassandra.db:type=CompactionManager", "forceUserDefinedCompaction", fais.keys.mkString(","))
//
//            fais.map { case (file, origLength) =>
////              n.jmxCommand("org.apache.cassandra.db:type=CompactionManager", "forceUserDefinedCompaction", file.getName())
//              val newFiles = watchForFileToRename(file, fais.keySet ++ transientFiles, dataFileFilter)
//              transientFiles ++= newFiles
//              val newLength = newFiles.foldLeft(0l)(_ + _.length())
//              FileDifferences(n, file, origLength, newLength)
//            }
            compactSpecificFilesTogether(ks, table, fais.keys.toSet)

          }
//          filesAndInitialSizes.map { case (file, origLength) =>
//            n.jmxCommand("org.apache.cassandra.db:type=CompactionManager", "forceUserDefinedCompaction", file.getName())
//            val newFiles = watchForFileToRename(file, filesAndInitialSizes.keySet ++ transientFiles, dataFileFilter)
//            transientFiles ++= newFiles
//            val newLength = newFiles.foldLeft(0l)(_ + _.length())
//            FileDifferences(n, file, origLength, newLength)
//          }
        }
      }
    }
    fileDifferences

  }

  def compactSpecificFilesTogether(ks: String, table: String, files: Set[File]): Seq[FileDifferences] = {
    CassandraSetup.clusters.flatMap { c =>
      c.datacenters.flatMap { d =>
        d.nodes.flatMap { n: CassandraNode =>
          val filesAndInitialSizes: Map[File, Long] = getDataFiles(c, d, n, ks, table)
          var transientFiles = filesAndInitialSizes.keySet
          val filteredFiles = filesAndInitialSizes.filter(f => files.contains(f._1))

          if (filteredFiles.nonEmpty) {
            n.jmxCommand("org.apache.cassandra.db:type=CompactionManager", "forceUserDefinedCompaction", filteredFiles.keySet.map(_.getName).mkString(","))
            val newFiles = watchForFileToRename(filteredFiles.keySet.head, filesAndInitialSizes.keySet ++ transientFiles, dataFileFilter)
            transientFiles ++= newFiles
            val newLength = newFiles.foldLeft(0l)(_ + _.length())

            Seq(FileDifferences(n, filesAndInitialSizes.keySet.head, filteredFiles.values.sum, newLength))
          } else {
            Seq.empty[FileDifferences]
          }
        }
      }
    }
  }

  case class beApproximatelyReducedToPct(pct: Double, tolerance: Double = 0.05) extends Matcher[FileDifferences] {
    def apply[S <: FileDifferences](s: Expectable[S]) =
      result(
        s.value.afterSize must be ~ ((s.value.origSize * pct).toLong +/- (s.value.origSize*tolerance).toLong),
        s.description + f" ${s.value.origFile.getName}'s size of ${s.value.afterSize}%,d is reduced by about ${pct*100}%.02f%% of ${s.value.origSize}%,d",
        s.description + f" ${s.value.origFile.getName}'s size of ${s.value.afterSize}%,d is NOT reduced by about ${pct*100}%.02f%% of ${s.value.origSize}%,d",
        s
      )
  }

  def printFileDifferences(differences: Seq[FileDifferences]): Unit = {
    differences.foreach { diff =>
//      logger.info(f"Compacted ${diff.origFile.getName} ${diff.origSize}%,d => ${diff.afterSize}%,d")
    }
  }

  def truncate(table: String)
              (implicit session: CQLSession): Unit = {
    Await.result(cql"TRUNCATE ${Inline(table)}".execute(), Duration.Inf)
  }

  def setupData(table: String, data: { def titles: List[String]; def rows: List[Product] })
               (implicit session: CQLSession) {

    val batch = CQLBatch(data.rows.map { row =>
      val kva = KeyValArgs(data.titles zip row.productIterator.toList)
      val st = cql"INSERT INTO ${Inline(table)} (${kva.keys}) VALUES (${kva.values})".prepare()
      st
    })

    Await.result(batch.execute(), Duration.Inf)
  }

  def setupTimestampedData(table: String, data: { def titles: List[String]; def rows: List[Product] })
                          (implicit session: CQLSession) {

    val batch = CQLBatch(data.rows.map { row =>
      val d = data.titles zip row.productIterator.toList
      val timestamp = d.find(_._1 == "timestamp").map(_._2).getOrElse(throw new Exception(s"timestamp not found in source data table $d"))
      val kva = KeyValArgs(d.filterNot(_._1 == "timestamp"))

      val st = cql"INSERT INTO ${Inline(table)} (${kva.keys}) VALUES (${kva.values}) USING timestamp $timestamp".prepare()
      st
    })

    Await.result(batch.execute(), Duration.Inf)
  }

}
