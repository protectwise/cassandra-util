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

import java.io.{FilenameFilter, File}
import java.nio.file._

import com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy
import com.protectwise.cql.CassandraClient
import com.protectwise.logging.Logging
import com.protectwise.testing.ccm.{CassandraCluster, CassandraNode, CassandraDC, CassandraSetup}
import com.typesafe.config.ConfigRenderOptions
import com.protectwise.cql._
import org.specs2.execute.Result
import org.specs2.matcher.{DataTables, Expectable, Matcher}
import org.specs2.time.NoTimeConversions

import scala.collection.immutable.Iterable
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import org.specs2.mutable.{BeforeAfter, Specification}
import scala.collection.JavaConverters._

class DeletingCompactionStrategySpec extends Specification with BeforeAfter with Logging with NoTimeConversions with DataTables with DeletingCompactionStrategySpecHelper {

  "DeletingCompactionStrategy" should {

    sequential

    "respond to a rules table" in {
      sequential
      implicit val session = client.session
      val ks = "testing"
      val table = "tenanted"

      // Go to deleting compaction strategy
      alterCompactionStrategy(ks, table, Map(
        "class" -> "com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy",
        "dcs_convictor" -> "com.protectwise.cassandra.retrospect.deletion.RuleBasedDeletionConvictor",
        "rules_select_statement" -> "SELECT rulename, column, range FROM testing.deletion_rules WHERE ks='testing' AND tbl='tenanted'",
        "dcs_underlying_compactor" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
        "dcs_backup_dir" -> "/tmp/backups",
        "dcs_status_report_ms" -> "1",
        "min_threshold" -> "2",
        "max_threshold" -> "2"
      ))

      "rule with multiple agents" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1726l    !! "00000151e518481a3b3e6939bc9493da" // X
        | 1726l    !! "00000151f43248180be67107251b6167" // X
        | 1726l    !! "00000151f84bd381fc78aca5e26567c0" // X
        | 1726l    !! "00000151fc18df688b31da3f0215d55f" // X
        | 1742l    !! "00000151cb75b17f7f53f2187da68102" // X
        | 1742l    !! "00000151cbb68f50d0833f75269cef4d" // X
        | 1742l    !! "00000151ce5d691144119389cabb2b6f" // X
        | 1742l    !! "00000151db1fdba2a888063457159d3e" // X
        | 1742l    !! "00000151dd6975e92eba7cdd4e4ecc12" // X
        | 1742l    !! "00000151f1470429cdf31d0485bb99e8" // X
        | 1742l    !! "00000151f4f221b20df147d1c14691b0" // X
        | 1742l    !! "00000151f7dabc8c37a47b98d3533552" // X
        | 1753l    !! "00000151d2b65a18a3aa7058aa6a2116" // kept because of agent
        | 1753l    !! "00000151f4da90041129071c9c9da7eb" // kept because of agent
        | 1768l    !! "00000151e1f2e6c5eb875453555c4e2e" // kept because of agent
        | 1768l    !! "00000151ffe7cc5df88bb70614edbbf0" // kept because of agent
        | 1771l    !! "000001520af1e48e75c67e3c82791880" // kept because of flow ID
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_1"    !! "tenant" !! (("1726", "1726"))
        | ks   !! table !! "cid_1"    !! "tenant" !! (("1742", "1742"))
        | ks   !! table !! "cid_1"    !! "tenant" !! (("1771", "1771"))
        | ks   !! table !! "cid_1"    !! "id"     !! ((null, "00000152000000000000000000000000"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 5)
      }

      "id range deletes correctly" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1782l    !! "00000151d54828e1775e4d044a7b5766" // X
        | 1782l    !! "00000151d587110b4d1a728565d72e30" // X
        | 1782l    !! "00000151d776e879a4ada06c8c839ef3" // X
        | 1782l    !! "00000151d9b90a33317258fef4bc49ed" // keep
        | 1782l    !! "00000151e25f060b49a8d79e897ab369" // keep
        | 1782l    !! "00000151e35ae631557c517014f885e1" // keep
        | 1782l    !! "00000151f08b0fc345cf5826903200cf" // keep
        | 1782l    !! "00000151f44306fcb6cadc71aaa3f372" // keep
        | 1782l    !! "00000151f9f0b9f98c5c9c616e4e3468" // keep
        | 1782l    !! "0000015203cb65b5c4a181d4222f7577" // keep
        | 1782l    !! "000001520a7fda1500bdf3318ef4ec95" // keep
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_2"    !! "tenant" !! (("1782", "1782"))
        | ks   !! table !! "cid_2"    !! "id"     !! ((null, "00000151d80000000000000000000000"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 8)
      }

      "cid range" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1785l    !! "00000151ef35589ff90c54401aae08f5" // X
        | 1785l    !! "0000015201153acb407978edcb28b452" // X
        | 1791l    !! "00000151eb66fc0c2ffc298022a0e221" // Keep
        | 1791l    !! "00000151f82053a2846086bd220a3abb" // Keep
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_3"    !! "tenant" !! (("1783", "1786"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 2)
      }

      "shortened id range" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1808l !! "000001500d6e500520483cb290acf7c4" // X
        | 1824l !! "00000150cbb694020320975300cc349c" // X
        | 1824l !! "00000150d92640fbc244d1670884f022" // X
        | 1824l !! "00000150e4d7b9bb0675764d10c0db0c" // Keep
        | 1824l !! "00000150e5984a8b2444310df745223c" // Keep
        | 1824l !! "00000150f89554064a02d4c7e86c58d3" // Keep
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_4"    !! "id"     !! (("", "00000150e0000000"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 3)
      }

      "full miss" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1879l !! "00000151ef32e8298ab774b3b950b97c" // Keep
        | 1883l !! "000001520d86a1830e6c40b6308c8aac" // Keep
        | 1884l !! "00000151df3e1efe7387e82c7c3d22a7" // Keep
        | 1885l !! "00000151e82201a09bcd632a95ca8d2d" // Keep
        | 1887l !! "00000151cb6c15d4cdceca118f9cbef1" // Keep
        )

        truncate("testing.deletion_rules")
        // No rules in the table, keep it all!

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 5)
      }

      "internal range" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1842l !! "00000151d923f0936f0f4b31e55a970d" // Keep
        | 1842l !! "00000151f641b42540fe0258023693c8" // Keep
        | 1842l !! "00000152095ac91e0809ce0a2419304c" // X
        | 1842l !! "0000015209f75d0b86cb5717484ce0ea" // X
        | 1842l !! "000001520a0a63fbb0582435ae3efa5d" // Keep
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_6"    !! "id"     !! (("00000151ff", "000001520a"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 3)
      }

      "sweep for oversized values" in {
        truncate(s"$ks.$table")
        setupData(s"$ks.$table",
          "tenant" || "id"
        | 1827l !! "000151d4e6fec2fde7240e6c5e1fce00" // Delete
        | 1827l !! "00000151e992ed8adb38cfbc2e6c0c52" // Keep
        | 1827l !! "00000151f6cde9e92ea68f051f6d96b5" // Keep
        | 1838l !! "00000151fb731fff3aae3a1236a90551" // Keep
        | 1866l !! "00000151e560cec31329cd7cdc4ad37e" // Keep
        | 1866l !! "00000151f9cc533b6347f86571e34eda" // Keep
        | 1866l !! "0000015203ddcf3d1598b05a8f5f1c2f" // Keep
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_7"    !! "id"     !! (("00001f", null))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 6)
      }

    }




    "respond to a rules table for TTLs" in {
      sequential
      implicit val session = client.session
      val ks = "testing"
      val table = "tenanted_ttl"




      // Ages need to be in microseconds
      def aged(timeInSeconds: Long) = {
        System.currentTimeMillis() * 1000 - timeInSeconds * 1000000
      }

      "with default TTL" in {
        sequential
        def setup() = alterCompactionStrategy(ks, table, Map(
          "class" -> "com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy",
          "dcs_convictor" -> "com.protectwise.cassandra.retrospect.deletion.RuleBasedLateTTLConvictor",
          "rules_select_statement" -> s"SELECT rulename, column, range, ttl FROM testing.deletion_rules_ttl WHERE ks='$ks' AND tbl='$table'",
          "dcs_underlying_compactor" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
          "dcs_backup_dir" -> "/tmp/backups",
          "default_ttl" -> "300",
          "dcs_status_report_ms" -> "1",
          "min_threshold" -> "2",
          "max_threshold" -> "2"
        ))

        "rules based default TTL" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
              | 1785l    !! "00000151ef35589ff90c54401aae08f5" !! aged(600) // Dropped because of age
              | 1785l    !! "0000015201153acb407978edcb28b452" !! aged(30) // Kept because of age
              | 1791l    !! "00000151eb66fc0c2ffc298022a0e221" !! aged(600) // Dropped because of default ttl
              | 1791l    !! "00000151f82053a2846086bd220a3abb" !! aged(30) // Kept because of default ttl age
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range"       || "ttl"
              | ks   !! table !! "full_ttl" !! "tenant" !! (("1785","1785")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 2)
        }
      }


      "without default TTL" in {
        sequential

        // Go to deleting compaction strategy
        def setup() = alterCompactionStrategy(ks, table, Map(
          "class" -> "com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy",
          "dcs_convictor" -> "com.protectwise.cassandra.retrospect.deletion.RuleBasedLateTTLConvictor",
          "rules_select_statement" -> s"SELECT rulename, column, range, ttl FROM testing.deletion_rules_ttl WHERE ks='$ks' AND tbl='$table'",
          "dcs_underlying_compactor" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
          "dcs_backup_dir" -> "/tmp/backups",
          "dcs_status_report_ms" -> "1",
          "min_threshold" -> "2",
          "max_threshold" -> "2"
        ))

        "rule with multiple agents" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1726l    !! "00000151e518481a3b3e6939bc9493da" !! aged(600) // X
          | 1726l    !! "00000151f43248180be67107251b6167" !! aged(600) // X
          | 1726l    !! "00000151f84bd381fc78aca5e26567c0" !! aged(600) // X
          | 1726l    !! "00000151fc18df688b31da3f0215d55f" !! aged(600) // X
          | 1742l    !! "00000151cb75b17f7f53f2187da68102" !! aged(100) // Kept because of timestamp
          | 1742l    !! "00000151cbb68f50d0833f75269cef4d" !! aged(600) // X
          | 1742l    !! "00000151ce5d691144119389cabb2b6f" !! aged(600) // X
          | 1742l    !! "00000151db1fdba2a888063457159d3e" !! aged(-300) // Kept because of future timing
          | 1742l    !! "00000151dd6975e92eba7cdd4e4ecc12" !! aged(600) // X
          | 1742l    !! "00000151f1470429cdf31d0485bb99e8" !! aged(600) // X
          | 1742l    !! "00000151f4f221b20df147d1c14691b0" !! aged(600) // X
          | 1742l    !! "00000151f7dabc8c37a47b98d3533552" !! aged(600) // X
          | 1753l    !! "00000151d2b65a18a3aa7058aa6a2116" !! aged(600) // kept because of agent
          | 1753l    !! "00000151f4da90041129071c9c9da7eb" !! aged(30) // kept because of agent
          | 1768l    !! "00000151e1f2e6c5eb875453555c4e2e" !! aged(0) // kept because of agent
          | 1768l    !! "00000151ffe7cc5df88bb70614edbbf0" !! aged(10000) // kept because of agent
          | 1771l    !! "000001520af1e48e75c67e3c82791880" !! aged(6000) // X
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "sid_1726" !! "tenant" !! (("1726", "1726")) !! 300l
          | ks   !! table !! "sid_1742" !! "tenant" !! (("1742", "1742")) !! 300l
          | ks   !! table !! "sid_1771" !! "tenant" !! (("1771", "1771")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 6)
        }

        "id range deletes correctly" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1782l    !! "00000151d54828e1775e4d044a7b5766" !! aged(600) // X
          | 1782l    !! "00000151d587110b4d1a728565d72e30" !! aged(30) // Kept because of age
          | 1782l    !! "00000151d776e879a4ada06c8c839ef3" !! aged(600) // X
          | 1782l    !! "00000151d9b90a33317258fef4bc49ed" !! aged(0) // keep
          | 1782l    !! "00000151e25f060b49a8d79e897ab369" !! aged(0) // keep
          | 1782l    !! "00000151e35ae631557c517014f885e1" !! aged(0) // keep
          | 1782l    !! "00000151f08b0fc345cf5826903200cf" !! aged(0) // keep
          | 1782l    !! "00000151f44306fcb6cadc71aaa3f372" !! aged(0) // keep
          | 1782l    !! "00000151f9f0b9f98c5c9c616e4e3468" !! aged(0) // keep
          | 1782l    !! "0000015203cb65b5c4a181d4222f7577" !! aged(0) // keep
          | 1782l    !! "000001520a7fda1500bdf3318ef4ec95" !! aged(0) // keep
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "cid_2"    !! "tenant" !! (("1782", "1782")) !! 300l
          | ks   !! table !! "cid_2"    !! "id"     !! ((null, "00000151d80000000000000000000000")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 9)
        }

        "cid range" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1785l    !! "00000151ef35589ff90c54401aae08f5" !! aged(600) // X
          | 1785l    !! "0000015201153acb407978edcb28b452" !! aged(30) // Kept because of age
          | 1791l    !! "00000151eb66fc0c2ffc298022a0e221" !! aged(600) // Keep
          | 1791l    !! "00000151f82053a2846086bd220a3abb" !! aged(30) // Keep
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "cid_3"    !! "tenant" !! (("1783", "1786")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 3)
        }

        "full scan TTL" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
              | 1785l    !! "00000151ef35589ff90c54401aae08f5" !! aged(600) // X
              | 1785l    !! "0000015201153acb407978edcb28b452" !! aged(30) // Kept because of age
              | 1791l    !! "00000151eb66fc0c2ffc298022a0e221" !! aged(600) // X
              | 1791l    !! "00000151f82053a2846086bd220a3abb" !! aged(30) // Keep because of age
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
              | ks   !! table !! "full_ttl" !! "tenant" !! ((null,null)) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 2)
        }

        "shortened id range" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1808l !! "000001500d6e500520483cb290acf7c4" !! aged(600) // X
          | 1824l !! "00000150cbb694020320975300cc349c" !! aged(30) // Kept because of age
          | 1824l !! "00000150d92640fbc244d1670884f022" !! aged(600) // X
          | 1824l !! "00000150e4d7b9bb0675764d10c0db0c" !! aged(30) // Keep
          | 1824l !! "00000150e5984a8b2444310df745223c" !! aged(600) // Keep
          | 1824l !! "00000150f89554064a02d4c7e86c58d3" !! aged(30) // Keep
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "cid_4"    !! "id"     !! (("", "00000150e0000000")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 4)
        }

        "full miss" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1879l !! "00000151ef32e8298ab774b3b950b97c" !! aged(600) // Keep
          | 1883l !! "000001520d86a1830e6c40b6308c8aac" !! aged(30) // Keep
          | 1884l !! "00000151df3e1efe7387e82c7c3d22a7" !! aged(300) // Keep
          | 1885l !! "00000151e82201a09bcd632a95ca8d2d" !! aged(3000) // Keep
          | 1887l !! "00000151cb6c15d4cdceca118f9cbef1" !! aged(30000) // Keep
          )

          truncate("testing.deletion_rules_ttl")
          // No rules in the table, keep it all!

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 5)
        }

        "internal range" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1842l !! "00000151d923f0936f0f4b31e55a970d" !! aged(600) // Keep
          | 1842l !! "00000151f641b42540fe0258023693c8" !! aged(600) // Keep
          | 1842l !! "00000152095ac91e0809ce0a2419304c" !! aged(600) // X
          | 1842l !! "0000015209f75d0b86cb5717484ce0ea" !! aged(30) // Keep becausae of age
          | 1842l !! "000001520a0a63fbb0582435ae3efa5d" !! aged(600) // Keep
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "cid_6"    !! "id"     !! (("00000151ff", "000001520a")) !! 300l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 4)
        }

        "sweep for oversized values" in {
          setup()
          truncate(s"$ks.$table")
          setupTimestampedData(s"$ks.$table",
            "tenant" || "id" || "timestamp"
          | 1827l !! "000151d4e6fec2fde7240e6c5e1fce00" !! aged(600) // Delete
          | 1827l !! "00000151e992ed8adb38cfbc2e6c0c52" !! aged(600) // Keep
          | 1827l !! "00000151f6cde9e92ea68f051f6d96b5" !! aged(600) // Keep
          | 1838l !! "00000151fb731fff3aae3a1236a90551" !! aged(600) // Keep
          | 1866l !! "00000151e560cec31329cd7cdc4ad37e" !! aged(600) // Keep
          | 1866l !! "00000151f9cc533b6347f86571e34eda" !! aged(600) // Keep
          | 1866l !! "0000015203ddcf3d1598b05a8f5f1c2f" !! aged(600) // Keep
          )

          truncate("testing.deletion_rules_ttl")
          setupData("testing.deletion_rules_ttl",
            "ks" || "tbl" || "rulename" || "column" || "range" || "ttl"
          | ks   !! table !! "cid_7"    !! "id"     !! (("00001f", null)) !! 0l
          )

          compactAllFiles(ks, table)

          val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
          "right number of records after" ==> (result.one().getLong("c") mustEqual 6)
        }
      }
    }

    "example configurable conviction strategy seems to not suck" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.ConfigurableDeleter',
           |   'delete_keys': '{
           |     "a": [8,[null,5],["11",null]],
           |     "b": [18,[null,15],["21",null]],
           |     "c": "1",
           |     "d": []
           |   }',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'dcs_status_report_ms': '1',
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(0.05)).foreach
    }

    "handle a variety of PK layouts with different convictors" in {
      ( "Convictor"              || "Table"             || "ReductionPct" || "Tolerance"
      | "KeepAllItemsDeleter"    !! "singlepk_nock"     !! 0              !! 0f
      | "KeepAllItemsDeleter"    !! "singlepk_singleck" !! 0              !! 0f
      | "KeepAllItemsDeleter"    !! "singlepk_dualck"   !! 0              !! 0f
      | "KeepAllItemsDeleter"    !! "dualpk_nock"       !! 0              !! 0f
      | "KeepAllItemsDeleter"    !! "dualpk_singleck"   !! 0              !! 0f
      | "KeepAllItemsDeleter"    !! "dualpk_dualck"     !! 0              !! 0f
      | "AllPartitionKeyDeleter" !! "singlepk_nock"     !! 100            !! 0f
      | "AllPartitionKeyDeleter" !! "singlepk_singleck" !! 100            !! 0f
      | "AllPartitionKeyDeleter" !! "singlepk_dualck"   !! 100            !! 0f
      | "AllPartitionKeyDeleter" !! "dualpk_nock"       !! 100            !! 0f
      | "AllPartitionKeyDeleter" !! "dualpk_singleck"   !! 100            !! 0f
      | "AllPartitionKeyDeleter" !! "dualpk_dualck"     !! 100            !! 0f
      | "OddPartitionKeyDeleter" !! "singlepk_nock"     !! 50             !! 0.0f
      | "OddPartitionKeyDeleter" !! "singlepk_singleck" !! 50             !! 0.0f
      | "OddPartitionKeyDeleter" !! "singlepk_dualck"   !! 50             !! 0.0f
      | "OddPartitionKeyDeleter" !! "dualpk_nock"       !! 50             !! 0.0f
      | "OddPartitionKeyDeleter" !! "dualpk_singleck"   !! 50             !! 0.0f
      | "OddPartitionKeyDeleter" !! "dualpk_dualck"     !! 50             !! 0.0f
      | "AllAtomDeleter"         !! "singlepk_nock"     !! 100            !! 0f
      | "AllAtomDeleter"         !! "singlepk_singleck" !! 100            !! 0f
      | "AllAtomDeleter"         !! "singlepk_dualck"   !! 100            !! 0f
      | "AllAtomDeleter"         !! "dualpk_nock"       !! 100            !! 0f
      | "AllAtomDeleter"         !! "dualpk_singleck"   !! 100            !! 0f
      | "AllAtomDeleter"         !! "dualpk_dualck"     !! 100            !! 0f
      | "OddClusterKeyDeleter"   !! "singlepk_nock"     !! 0              !! 0f // no cluster key convicts nothing
        // This configuration seems problematic, currently believe it uncovers bugs in underlying Cassandra implementation details.
        // Even though we only delete odd cluster keys, somehow in this one scenario, even cluster keys get removed
        // from the index as well.  If you force a reindex, the correct data reappears.  I can't explain it but
        // I think the deleting code is working correctly WRT index cleanup, but the underlying implementation blorks
        // something it shouldn't.
      | "OddClusterKeyDeleter"   !! "singlepk_singleck" !! 50             !! 0.01f
      | "OddClusterKeyDeleter"   !! "singlepk_dualck"   !! 50             !! 0.0f
      | "OddClusterKeyDeleter"   !! "dualpk_nock"       !! 0              !! 0f // no cluster key convicts nothing
      | "OddClusterKeyDeleter"   !! "dualpk_singleck"   !! 50             !! 0.0f
      | "OddClusterKeyDeleter"   !! "dualpk_dualck"     !! 50             !! 0.0f
      ) |> {
        case (convictor, table, reduction, tolerance) =>
          implicit val session = client.session
          val ks = "testing"

          setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

          val initialCount = Await.result(cql"SELECT count(*) AS cnt FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf).one().getLong("cnt")

          // Go to deleting compaction strategy
          Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.${Inline(convictor)}',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'dcs_backup_dir': '/tmp/backups',
           |   'dcs_status_report_ms': '1',
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

          val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

          printFileDifferences(fileDifferences)
          val afterCount = Await.result(cql"SELECT count(*) AS cnt FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf).one().getLong("cnt")

          s"records reduced by $reduction pct" ==> (
            afterCount must be ~ ((initialCount * (1f - (reduction.toFloat/100))).toLong +/- 1)
          )


          s"file sizes reduced by $reduction pct" ==> (
            fileDifferences must contain(beApproximatelyReducedToPct(1f-(reduction.toFloat/100), tolerance)).foreach
            )

          def testSolr = {
            val solrCount = Await.result( cql"""
              | SELECT count(*) AS cnt FROM ${Inline(ks)}.${Inline(table)}
              | WHERE solr_query='{"q":"a:*","commit":true}'
            """.stripMargin.execute(), Duration.Inf).one().getLong("cnt")

            s"solr records reduced by $reduction pct" ==> (
              solrCount must be ~ (
                (initialCount * (1f - (reduction.toFloat/100))).toLong +/- 1)
            )
          }

          if (convictor.equals("OddClusterKeyDeleter") && table.equals("singlepk_singleck")) {
            testSolr.pendingUntilFixed("There seems to be a bug in Cassandra related to correctly cleaning up indexes on single PK, single CK combos where too many records get removed from the index.")
          } else {
            testSolr
          }

          // Go back to a default compaction strategy - this is mostly to help out tools like sstabledump which get
          // spooked because the jar containing our compaction strategy isn't part of the core.
          alterCompactionStrategy(ks, table)

          success

      }

    }


    "handle conviction removing some clusters" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 2, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.OddClusterKeyDeleter',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(0.5, 0.1)).foreach
    }

    "handle conviction removing all partitions" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.AllPartitionKeyDeleter',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(0, 0)).foreach
    }

    "handle conviction removing all cluster keys" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.AllClusterKeyDeleter',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(0, 0)).foreach
    }

    "delete no partition keys for dry run option" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.AllPartitionKeyDeleter',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'dcs_is_dry_run': true,
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(1, 0)).foreach
    }

    "delete no cluster keys for dry run option" in {
      implicit val session = client.session
      val ks = "testing"
      val table = "singlepk_singleck"

      setupTestTable(ks, table, abRangeData(ks, table, 0 until 100, 0 until 100))

      // Go to deleting compaction strategy
      Await.result(cql"""ALTER TABLE ${Inline(ks)}.${Inline(table)} WITH
           | compaction = {
           |   'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
           |   'dcs_convictor': 'com.protectwise.cassandra.db.compaction.example.AllClusterKeyDeleter',
           |   'dcs_underlying_compactor': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
           |   'dcs_is_dry_run': true,
           |   'min_threshold': '4',
           |   'max_threshold': '32'
           | }
           """.stripMargin.execute(), 100.seconds)

      val fileDifferences: Seq[FileDifferences] = compactAllFiles(ks, table)

      printFileDifferences(fileDifferences)

      fileDifferences must contain(beApproximatelyReducedToPct(1, 0)).foreach
    }

  }





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

  lazy val onlyOnce = CassandraSetup.doAllSetup()
  lazy val client = CassandraClient("default")

  override def after: Any = {}

  override def before: Any = {
    onlyOnce
  }

}
