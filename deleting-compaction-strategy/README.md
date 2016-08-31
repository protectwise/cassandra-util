# Cassandra Deleting Compaction Strategy

## Building

To produce a jar which can be loaded into your Cassandra cluster:

    sbt package
    
This will place a jar at `deleting-compaction-strategy/target/scala-2.10/deleting-compaction-strategy-0.24-SNAPSHOT.jar`.  Although the target path mentions Scala, there are no Scala dependencies in this jar, and in fact there are no dependencies which are not already part of Cassandra core.
