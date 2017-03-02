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

class RuleBasedDeletionConvictorSpec extends Specification with Logging with NoTimeConversions with DataTables with DeletingCompactionStrategySpecHelper {
  "RuleBasedDeletionConvictor" should {
    implicit val session = client.session
    val ks = "testing"
    val table = "tenanted"

    // Go to deleting compaction strategy
    def setup(k: String=ks, t: String=table) = alterCompactionStrategy(k, t, Map(
      "class" -> "com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy",
      "dcs_convictor" -> "com.protectwise.cassandra.retrospect.deletion.RuleBasedDeletionConvictor",
      "rules_select_statement" -> s"SELECT rulename, column, range FROM testing.deletion_rules WHERE ks='$k' AND tbl='$t'",
      "dcs_underlying_compactor" -> "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
      "dcs_backup_dir" -> "/tmp/backups",
      "dcs_status_report_ms" -> "1",
      "min_threshold" -> "2",
      "max_threshold" -> "2"
    ))
    "respond to a rules table" in {
      sequential

      "rule with multiple tenants" in {
        setup()
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
            | 1753l    !! "00000151d2b65a18a3aa7058aa6a2116" // kept because of tenant
            | 1753l    !! "00000151f4da90041129071c9c9da7eb" // kept because of tenant
            | 1768l    !! "00000151e1f2e6c5eb875453555c4e2e" // kept because of tenant
            | 1768l    !! "00000151ffe7cc5df88bb70614edbbf0" // kept because of tenant
            | 1771l    !! "000001520af1e48e75c67e3c82791880" // kept because of ID
        )

        truncate("testing.deletion_rules")
        setupData("testing.deletion_rules",
          "ks" || "tbl" || "rulename" || "column" || "range"
        | ks   !! table !! "cid_1"    !! "tenant" !! (("1726", "1726"))
        | ks   !! table !! "cid_2"    !! "tenant" !! (("1742", "1742"))
        | ks   !! table !! "cid_3"    !! "tenant" !! (("1771", "1771"))
        | ks   !! table !! "cid_3"    !! "id"     !! ((null, "00000152000000000000000000000000"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 5)
      }

      "id range deletes correctly" in {
        setup()
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
        setup()
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
        setup()
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
        setup()
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
        setup()
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
             "ks"  || "tbl" || "rulename" || "column" || "range"
            | ks   !! table !! "cid_6"    !! "id"     !! (("00000151ff", "000001520a"))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 3)
      }

      "sweep for oversized values" in {
        setup()
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
             "ks"  || "tbl" || "rulename" || "column" || "range"
            | ks   !! table !! "cid_7"    !! "id"     !! (("00001f", null))
        )

        compactAllFiles(ks, table)

        val result = Await.result(cql"SELECT count(*) AS c FROM ${Inline(ks)}.${Inline(table)}".execute(), Duration.Inf)
        "right number of records after" ==> (result.one().getLong("c") mustEqual 6)
      }

    }


  }
}
