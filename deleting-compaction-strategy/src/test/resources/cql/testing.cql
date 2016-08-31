CREATE KEYSPACE testing WITH REPLICATION =  { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE testing.singlepk_nock (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY (a)
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.singlepk_singleck (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY (a,b)
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.singlepk_dualck (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY ((a),b,c)
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.dualpk_nock (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY ((a,b))
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.dualpk_singleck (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY ((a,b),c)
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.dualpk_dualck (
    a int,
    b int,
    c int,
    d int,
    PRIMARY KEY ((a,b),c,d)
) WITH
    compression = { 'sstable_compression': '' }
AND compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };


CREATE TABLE testing.deletion_rules (
    ks text,
    tbl text,
    rulename text,
    column text,
    range frozen<tuple<text,text>>,
    PRIMARY KEY((ks, tbl), rulename, column, range)
) WITH
    compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };

CREATE TABLE testing.deletion_rules_ttl (
    ks text,
    tbl text,
    rulename text,
    column text,
    range frozen<tuple<text,text>>,
    ttl bigint,
    PRIMARY KEY((ks, tbl), rulename, column)
) WITH
    compaction = {
     'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
     'min_threshold': 2,
     'max_threshold': 2
   };



CREATE TABLE testing.tenanted (
    tenant bigint,
    id text,
    PRIMARY KEY ((id, tenant))
) WITH
 compaction = {
   'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
   'min_threshold': '2',
   'max_threshold': '2'
 };

CREATE TABLE testing.tenanted_ttl (
    tenant bigint,
    id text,
    PRIMARY KEY ((id, tenant))
) WITH
 compaction = {
   'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
   'min_threshold': '2',
   'max_threshold': '2'
 };
