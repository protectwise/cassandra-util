# ProtectWise Cassandra Util
A series of utilities for working with Apache Cassandra.

## CCM Testing Helper
Testing fixtures for interacting with CCM for automated integration tests.  Use configuration to define a set of cluster topologies and let these classes rather than a sequence of esoteric CCM commands.

See [reference.conf](ccm-testing-helper/src/main/resources/reference.conf) for example configurations.

## CQL Wrapper
A wrapper on the DataStax Java Driver for use from Scala applications.  Cluster connections driven by configuration so your DevOps team will love you.  Also powerful options for tuning performance and achieving parallelism, as well as implicit arguments for execution context, consistency, retry policy, and sessions.

See [CQL Wrapper Documentation](cql-wrapper/README.md)

## Deleting Compaction Strategy
A custom compaction strategy for Cassandra which allows for data to be removed during standard compaction without resorting to tombstones or TTL's.  Currently supports Cassandra 2.x series.

See [Deleting Compaction Strategy Documentation](deleting-compaction-strategy/README.md)

## ProtectWise Util
Support classes copied in from ProtectWise internal repositories.