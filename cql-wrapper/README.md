# DataStax Java Driver Wrapper for Scala

## Introduction
This is a lightweight wrapper for the DataStax Java Driver which provides a more Scala-friendly feature set.  All calls are delegated out to the DataStax Java Driver under the hood.

## Features
* Connections to Cassandra clusters are defined in configuration rather than through builders, which is much friendlier for your DevOps team.
* Work with Scala types rather than Java types, including primitives, collections, and especially Futures
* Automatic prepared statement caching (calling `.prepare()` on a statement does not re-prepare that statement if an identical statement had previously been prepared)
* CQL String context to simplify the boilerplate of statement construction, such as `cql"SELECT * FROM foo WHERE id = 7"`
* Simplified parameter binding: `cql"SELECT * FROM foo WHERE id = $someId"` or `val statement=cql"SELECT * FROM foo WHERE id=${'id}"; ... statement.withValues('id -> 7)`
* Token aware batching - group statements by primary replica and execute the logical batch as child batches where each statement is necessarily owned by the coordinator.
* Implicit sessions, execution contexts, consistency levels, and retry policies - set them once at the root of your execution tree and forget about them.
* Built in tracing hooks - wire up Zipkin or something else.  If you execute a statement with server side tracing enabled, those trace results will also be bubbled up to your tracer automatically.
* Built in metrics hooks - wire up metrics to Codahale or any other tracing library.

## Configuring a Connection
Connections are defined in TypeSafe style config (see https://github.com/typesafehub/config) under `cassandra.connections.{connectionName}`, and any properties which are not provided are left to the driver default value.

Each connection entry, an object of properties can be configured according to below, most properties are optional, where not provided, they default to the driver default policy.

    cassandra.connections.some-connection = {
        seeds: List[String]
        clustername: String
        port: Int
        keyspace: String
        
        ssl.enabled: Boolean
        ssl.protocol: String
        ssl.provider: String
        ssl.cipherSuites: List[String]
        ssl.truststore: String
        ssl.truststorePassword: String
        ssl.keystore: String
        ssl.keystorePassword: String
        
        auth.username: String
        auth.password: String
        
        pool.local.coreConnectionsPerHost: Int
        pool.local.maxConnectionsPerHost: Int
        pool.local.maxSimultaneousRequestsPerConnectionThreshold: Int
        pool.local.minSimultaneousRequestsPerConnectionThreshold: Int
        pool.remote.coreConnectionsPerHost: Int
        pool.remote.maxConnectionsPerHost: Int
        pool.remote.maxSimultaneousRequestsPerConnectionThreshold: Int
        pool.remote.minSimultaneousRequestsPerConnectionThreshold: Int
        
        socket.connectTimeout: Milliseconds
        socket.keepAlive: Boolean
        socket.readTimeoutMillis: Milliseconds
        socket.receiveBufferSize: Bytes
        socket.reuseAddress: Boolean
        socket.sendBufferSize: Bytes
        socket.soLingerSecs: Int
        socket.tcpNoDelay: Boolean
        
        retry.policy: Enum[DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy]
        retry.log: Boolean
        
        loadbalancer: { ... } // See Policy Definition below
        
        // See Batch Size Controls below
        max_unlogged_batch_statements: Int
        max_counter_batch_statements: Int
        
        copyFrom: String // Name of another configuration to copy default values from, see Copying Config below. 
    }

### LoadBalancer Policy Definition

    // DCAwareRoundRobinPolicy - see http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html
    { 
      type=DCAwareRoundRobinPolicy
      localDc: String
      usedHostsPerRemoteDc: Int
      allowRemoteDCsForLocalConsistencyLevel: Boolean - NOT RECOMMENDED!!  See http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html#DCAwareRoundRobinPolicy(java.lang.String,%20int,%20boolean)
    }
    
    // LatencyAwarePolicy - see http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/LatencyAwarePolicy.html
    { 
      type=LatencyAwarePolicy
      exclusionThreshold: Double
      minimumMeasurements: Int
      retryPeriod: Duration
      scale: Duration
      updateRate: Duration
      childPolicy: { ... } // Policy Definition [REQUIRED]
    }
    
    // TokenAwarePolicy - see http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/TokenAwarePolicy.html
    { 
      type=TokenAwarePolicy
      childPolicy: { ... } // Policy Definition [REQUIRED]
    }
    
    // WhiteListPolicy - see http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/WhiteListPolicy.html
    {
      type=WhiteListPolicy
      whitelist: List[String] [REQUIRED]
      childPolicy: { ... } // Policy Definition [REQUIRED]
    }
    
    // RoundRobinPolicy - see http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/RoundRobinPolicy.html
    { 
      type=RoundRobinPolicy
    }
    
    // Default is:
    loadbalancer = {
      type = TokenAwarePolicy
      childPolicy = {
        type = DCAwareRoundRobinPolicy
      }
    }

### Batch Size Controls
_See cluster config properties `max_unlogged_batch_statements` and `max_counter_batch_statements`._

One of the features unique to this driver is the ability to separate logical batches from the application's perspective from actual batches executed on the cluster.  This is important because optimum batch size can differ wildly depending on the version of Cassandra in use.  With these controls you can tune batch sizes per connection without adjusting application logic.
 
When batches larger than the configured size are submitted by the application, the batches are broken up into smaller batches of the appropriate size, then executed _concurrently_.  The entire result future completes only after the set of all batches has completed.

If you set max batch size to 1, the logical batches are executed instead as individual statements (rather than single-statement batches).  This is also true for single statement remainders of sub-batches.

You cannot configure match batch size for logged batches as this would violate the atomicity assumption assumptions of a logged batch.

### Copying Config
For short handing common configuration options, you can copy configuration from another connection.  This will let you save the overhead and error prone state of copypasta config options.

The copy happens before local config applies, regardless of where in the local config the `copyFrom` appears.  I.E. local config always has priority.

Example:

    cassandra.connections = {
        "_base" = {
          auth.username = "some-user"
          auth.password = "some-password"
          retry.policy = DowngradingConsistencyRetryPolicy
          ssl = {
            enabled = true
            provider = "SSL"
            keystore = "some-keystore.jks"
            keystorePassword = "keystore-password"
            truststore = "some-truststore.jks"
            truststorePassword = "truststore-password"
          }
          max_unlogged_batch_statements = 100
          max_counter_batch_statements = 100
        }
        real-connection = {
          copyFrom = "_base"
          seeds = ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
        }
        another-connection = {
          copyFrom = "_base"
          seeds = ["4.3.2.1", "8.7.6.5", "12.11.10.9"]
        }
        big-batches = {
          copyFrom = "another-connection"
          max_unlogged_batch_statements = 99999999
        }
        no-batches = {
          copyFrom = "another-connection"
          max_unlogged_batch_statements = 1
          max_counter_batch_statements = 1
        }
    }
    

# CQL String Context
A string context is provided to make statement construction simple

## Simple statement construction

    import com.protectwise.cql._
    val statement: CQLStatement = cql"SELECT * FROM foo"

You can also use triplequotes and stripMargin like most other string contexts:

    val statement: CQLStatement = 
      cql"""SELECT *
           |FROM   foo
           |WHERE  id = 7
           |""".stripMargin

If you need to dynamically build up a statement you can add multiple statements together, or even just concatenate a string:

    val statement: CQLStatement = cql"SELECT * FROM " + (
      resolution match {
        case DAILY =>  cql"daily_aggregate" // concatenate two cql statements together
        case HOURLY => "hourly_aggregate"   // concatenate a cql statement with a string
      }
    )

## Parameter binding

All variable substitutions are injected as bound parameters by default.  This is to make it difficult to introduce a CQL injection attack into your codebase.  Note that `.withValues()` is similar to a case class `.copy()` command in that you will be given a new statement instance with locally bound variables, so that a single base statement can be safely shared across threads.

    // Single CQLStatement with positional bound parameters
    val statement = cql"SELECT * FROM foo WHERE id = ${}"
    ...
    statement.withValues(7)
    
    // Single CQLStatement with a pre-bound parameter
    val statement = cql"SELECT * FROM foo WHERE id = ${7}"
    // or
    val statement = cql"SELECT * FROM foo WHERE id = $someVar"
    
    // Single CQLStatement with a name-bound parameter
    val statement = cql"SELECT * FROM foo WHERE id = ${'id}"
    ...
    statement.withValues('id -> 7)

If you need to inject dynamic CQL which is not a bound parameter, use the `Inline()` helper:

    val (tableName, fieldList) = resolution match {
      case DAILY  => "daily_aggregate"  -> "year, month, day, value"
      case HOURLY => "hourly_aggregate" -> "year, month, day, hour, value"
    }
    val statement = cql"SELECT ${Inline(fieldList)} FROM ${Inline(tableName)} WHERE id = $someVar"

You can also use `Inline()` for bound positions (substitutions happen client side):

    val statement = cql"SELECT ${'fields} FROM daily_aggregate WHERE id = ${'id}"
    ...
    statement.withValues('fields -> Inline(fields), 'id -> 7)

## Prepared statements

Prepared statements can be produced by taking any standard statement and calling `.prepare()` on it.  Note that preparation doesn't actually happen at the time when `.prepare()` is called, but rather lazily just before execution.  

You can call `.prepare()` as many times as you like on a statement, or on as many different statement instances as you like which have the same materialized CQL (minus bound parameters), a prepared statement cache will first check if there is any work to be done prior to execution.  This prepared statement cache is maintained per connection, so you can reuse a prepared statement across multiple Cassandra connections, it will be prepared exactly once for each connection, just in time for execution.

However you should note that it is generally a good idea to build up your statements just one time and reuse them as much as possible.

# Batches

Batches are simply logical groupings of statements which are executed as a single unit from the application's perspective.  Batches can be Logged, Unlogged, or Counter batches.  We think of batches as either Logical batches, which are a single unit of work to the application, and sub-batches, which are single units of work to the Cassandra cluster.  For Logged batches, there is no difference.

Some simple things you can do with batches:

    // Simple unlogged batch
    CQLBatch(statements)
    
    // Counter batch
    CQLBatch(statements).counter
    
    // Logged batch
    CQLBatch(statements).logged
    
    // Add two batches together to make a single batch:
    val finalBatch: CQLBatch = someBatch ++ anotherBatch
    
    // Add statements to a given batch
    val newBatch = existingBatch ++ cql"SELECT * FROM foo"
    

## Differences from DataStax Java Driver batches

* Application logical batches may be subdivided up and delivered as smaller sub-batches executed concurrently, as directed by connection configuration
* Batches may be _read_ batches, and you can iterate the full resultset either in statement order or interleaved.
* Batches can be executed in a token aware manner (where statements in the batch are first grouped by their primary replica, then those sub-batches are individually executed)

# Execution

The synchronous execution methods from the underlying DataStax Java Driver are not exposed to this API.  Blocking threads in Scala is a bad idea and should be avoided.  If you really need to block, you can use something like `Await.ready()`, however you should reevaluate your life choices if so.

