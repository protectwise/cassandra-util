# Cassandra Deleting Compaction Strategy

Deleting compaction is a way to achieve deletes in Apache Cassandra without resorting to tombstones or TTLs.  It works by establishing rules for what data should be deleted, then during either standard or user-defined compaction, preventing records that fail conviction from entering the newly compacted SSTable.  Secondary indexes are cleaned up at the time that deletion occurs.  




## Deleting Compaction Strategy Options
DCS base compaction options:
* `dcs_convictor` - required string - what class convicts records during compaction. Must be a valid class name that extends AbstractSimpleDeletingConvictor
* `dcs_underlying_compactor` - required string - the underlying compaction strategy to use for sstable selection
* `dcs_is_dry_run` - optional boolean - if true, no deletes are performed, instead statistics about what would have been deleted are logged to Cassandra's logs
* `dcs_status_report_ms` - optional int - if provided, a status report about the progress of deleting compaction will be logged at this interval
* `dcs_backup_dir` - optional string - if provided, records deleted during compaction will be written to SSTable files in this directory.

All other options are passed first to the convictor class, then to the underlying compaction strategy.

## Convictors
The DCS convictors can have their own options.  For example:

### ConfigurableDeleter
* `delete_keys` - required string - json string dictionary providing deletion rules in the form of `{"columnName":[range,range,...], "columnName": ... }`, where ranges are defined as either a single value, or a two-value array of upper and lower bounds to match for this column, where a null value indicates an unbounded edge of the range.

### RuleBasedLateTTLConvictor

* `rules_select_statement` - required string - CQL SELECT statement to retrieve late TTL rules. The statement must return the following columns:
  * `column` (text)
  * `rulename` (any)
  * `range` (tuple2<string,string>)
  * `ttl` (bigint)

For given rulename, all columns in that rule must be either partition or cluster keys, and in order for the rule to apply to the row, each column in the rule must match at least one range. If multiple rules match a row, the rule with the lowest TTL will apply. TTL of 0 means delete immediately, TTL < 0 means do not delete. The atoms writetime is compared against the effective TTL, and if the atom is older than the TTL, the atom does not survive compaction.

#### Example
For a table that includes a field named "tenant" in in the primary key (either partition or cluster), we can apply a compaction-time TTL like this.  Note the last lines where we wrap SizeTieredCompactionStrategy - this should work the same way with any other compaction strategy.

    ALTER TABLE foo.bar 
    WITH compaction = {
        'class': 'com.protectwise.cassandra.db.compaction.DeletingCompactionStrategy',
        'dcs_convictor': 'com.protectwise.cassandra.retrospect.deletion.RuleBasedLateTTLConvictor',
        'rules_select_statement': 'SELECT rulename, column, range, ttl FROM entitlement.deletion_rules_ttl WHERE ks=''all'' AND tbl=''customer''',
        'dcs_status_report_ms': '60000',
        'dcs_is_dry_run': 'false',
        'dcs_backup_dir': '/mnt/deletion-backups/foo.bar/',
        'dcs_underlying_compactor': 'SizeTieredCompactionStrategy',
        'min_threshold': '2',
        'max_threshold': '8'
    };

With accompanying deletion rules which look like this:

    cqlsh> SELECT * FROM entitlement.deletion_rules_ttl
    ks   | tbl      | rulename | column | range            | ttl
    -----+----------+----------+--------+------------------+----------
    all  | customer | cid_1327 | tenant | ('1327', '1327') | 2678400
    all  | customer | cid_1769 | tenant | ('1769', '1769') | 0
    all  | customer | cid_1770 | tenant | ('1770', '1770') | 2678400
    all  | customer | cid_1773 | tenant | ('1773', '1773') | 0
    all  | customer | cid_1774 | tenant | ('1774', '1774') | 32140800
    all  | customer | cid_1775 | tenant | ('1775', '1775') | 2678400
    all  | customer | cid_1777 | tenant | ('1777', '1777') | 32140800
    all  | customer | cid_1783 | tenant | ('1783', '1783') | 2678400
    all  | customer | cid_1784 | tenant | ('1784', '1784') | 2678400
    all  | customer | cid_1785 | tenant | ('1785', '1785') | 0


## Building

To produce a jar which can be loaded into your Cassandra cluster:

    sbt package
    
This will place a jar at `deleting-compaction-strategy/target/scala-2.10/deleting-compaction-strategy-0.24-SNAPSHOT.jar`.  Although the target path mentions Scala, there are no Scala dependencies in this jar, and in fact there are no dependencies which are not already part of Cassandra core.
