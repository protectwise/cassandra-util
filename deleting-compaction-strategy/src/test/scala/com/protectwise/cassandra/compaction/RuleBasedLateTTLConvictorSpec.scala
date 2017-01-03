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

import com.protectwise.cql.Inline
import com.protectwise.logging.Logging
import com.protectwise.cql._
import org.specs2.matcher.DataTables
import org.specs2.mutable.{BeforeAfter, Specification}
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RuleBasedLateTTLConvictorSpec extends Specification with Logging with NoTimeConversions with DataTables with DeletingCompactionStrategySpecHelper {

  "RuleBasedLateTTLConvictor" should {
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


}
