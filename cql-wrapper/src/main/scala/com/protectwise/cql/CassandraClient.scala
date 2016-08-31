/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cql

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.TimeUnit
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import com.datastax.driver.core
import com.datastax.driver.core.Host.StateListener
import com.datastax.driver.core.policies._
import com.datastax.driver.core.{Configuration => DSConfiguration, _}
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.protectwise.cql.metrics.CQLMetrics
import com.protectwise.cql.tracing.{CQLTracer, NullTracer}
import com.protectwise.logging.Logging
import com.protectwise.util.Configuration
import com.protectwise.util.Configuration.MissingConfigurationException

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object CassandraClient extends Logging {
  val rootConfig = Configuration.load().getConfig("cassandra") getOrElse Configuration.empty
  val clients: TrieMap[String, CassandraClient] = TrieMap()

  def missingConfig[T](name: String): T = {
    throw new MissingConfigurationException(name)
  }

  def getConfig(name: String, copyHistory:Seq[String] = Seq()): Configuration = {
    val base = rootConfig.getConfig(s"connections.$name") getOrElse { throw new Exception(s"Missing cassandra connection entry cassandra.connections.$name") }
    val copy = base.getString("copyFrom") map { copyName =>
      if (copyHistory.contains(copyName)) throw new Exception(s"Cyclical cassandra connection entry ${copyHistory.mkString(" => ")} references $name in copyFrom attribute")
      getConfig(copyName, copyHistory :+ copyName)
    } getOrElse Configuration.empty
    copy ++ base
  }

  def apply(name: String, tracer: CQLTracer = NullTracer, metrics: Seq[CQLMetrics] = Seq()): CassandraClient = clients.synchronized {
    clients.getOrElseUpdate(name, {
      val config = getConfig(name)
      val cluster = {

        val builder = Cluster.builder()
        // seeds: List[String]
        builder.addContactPoints(config.getStringList("seeds").getOrElse(missingConfig("seeds")): _*)
        // clustername: String
        config.getOpt[String]("clustername").map(builder.withClusterName)
        // port: Int
        config.getInt("port").map(builder.withPort)
        // keyspace: String (referenced by session later)
        // preparedStatementsCacheSize: Int

        // ssl.enabled: Boolean
        // ssl.protocol: String
        // ssl.provider: String
        // ssl.cipherSuites: List[String]
        // ssl.truststore: String
        // ssl.truststorePassword: String
        // ssl.keystore: String
        // ssl.keystorePassword: String
        config.getConfig("ssl").map {
          case cfg if cfg.getBoolean("enabled") getOrElse true =>


            val context = (cfg.getOpt[String]("protocol"), cfg.getOpt[String]("provider")) match {
              case (None, _) if (cfg.getString("truststore") ++ cfg.getString("keystore")).isEmpty => Some(SSLContext.getDefault)
              case (None, _) => Some(SSLContext.getInstance("SSL"))
              case (Some(protocol), None) => Some(SSLContext.getInstance(protocol))
              case (Some(protocol), Some(provider)) => Some(SSLContext.getInstance(protocol, provider))
              case _ => None
            }

            val truststore = (cfg.getString("truststore"), cfg.getString("truststorePassword")) match {
              case (Some(tsPath), Some(tspw)) =>
                val tsf = Try {
                  new FileInputStream(tsPath)
                } getOrElse {
                  getClass.getClassLoader.getResourceAsStream(tsPath)
                }
                val ts = KeyStore.getInstance("JKS")
                ts.load(tsf, tspw.toCharArray)
                val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
                tmf.init(ts)
                Some(tmf)
              case (None, None) => None
              case _ => throw new Exception("You must either provide both ssl.truststore plus ssl.truststorePassword, or neither")
            }

            val keystore = (cfg.getString("keystore"), cfg.getString("keystorePassword")) match {
              case (Some(keystorePath), Some(password)) =>
                val ksf = Try {
                  new FileInputStream(keystorePath)
                } getOrElse {
                  getClass.getClassLoader.getResourceAsStream(keystorePath)
                }
                val ks = KeyStore.getInstance("JKS")
                ks.load(ksf, password.toCharArray)
                val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
                kmf.init(ks, password.toCharArray)
                Some(kmf)
              case (None, None) => None
              case _ => throw new Exception("You must provide either ssl.keystore plus ssl.keystorePassword, or neither")
            }

            context.map(ctx => ctx.init(keystore.map(_.getKeyManagers) getOrElse Array(), truststore.map(_.getTrustManagers) getOrElse Array(), new SecureRandom()))

            val options = (context, cfg.getStringList("cipherSuites")) match {
              case (None, None) => None
              case (Some(ctx), None) => Some(JdkSSLOptions.builder().withSSLContext(ctx).build())
              case (None, Some(suites)) => Some(JdkSSLOptions.builder().withCipherSuites(suites.toArray).build())
              case (Some(ctx), Some(suites)) => Some(JdkSSLOptions.builder().withSSLContext(ctx).withCipherSuites(suites.toArray).build())
            }
            options.map(builder.withSSL)
          case _ =>
        }

        // auth.username: String
        // auth.password: String
        (config.getOpt[String]("auth.username"), config.getOpt[String]("auth.password")) match {
          case (Some(un), Some(pw)) => builder.withCredentials(un, pw)
          case (Some(_), _) => throw new Exception(s"auth.username provided, but no auth.password found for Cassandra connection $name")
          case (_, Some(_)) => throw new Exception(s"No auth.username provided, but auth.password found for Cassandra connection $name")
          case (_, _) => // No auth selected
        }

        // pool.local.coreConnectionsPerHost: Int
        // pool.local.maxConnectionsPerHost: Int
        // pool.local.maxSimultaneousRequestsPerConnectionThreshold: Int
        // pool.local.minSimultaneousRequestsPerConnectionThreshold: Int
        // pool.remote.coreConnectionsPerHost: Int
        // pool.remote.maxConnectionsPerHost: Int
        // pool.remote.maxSimultaneousRequestsPerConnectionThreshold: Int
        // pool.remote.minSimultaneousRequestsPerConnectionThreshold: Int
        builder.withPoolingOptions({
          val policy = new core.PoolingOptions()
          config.getInt("pool.local.coreConnectionsPerHost").map((policy.setCoreConnectionsPerHost _).curried(HostDistance.LOCAL))
          config.getInt("pool.local.maxConnectionsPerHost").map((policy.setMaxConnectionsPerHost _).curried(HostDistance.LOCAL))
//          config.getInt("pool.local.maxSimultaneousRequestsPerConnectionThreshold").map((policy.setMaxSimultaneousRequestsPerConnectionThreshold _).curried(HostDistance.LOCAL))
//          config.getInt("pool.local.minSimultaneousRequestsPerConnectionThreshold").map((policy.setMinSimultaneousRequestsPerConnectionThreshold _).curried(HostDistance.LOCAL))
          config.getInt("pool.remote.coreConnectionsPerHost").map((policy.setCoreConnectionsPerHost _).curried(HostDistance.REMOTE))
          config.getInt("pool.remote.maxConnectionsPerHost").map((policy.setMaxConnectionsPerHost _).curried(HostDistance.REMOTE))
//          config.getInt("pool.remote.maxSimultaneousRequestsPerConnectionThreshold").map((policy.setMaxSimultaneousRequestsPerConnectionThreshold _).curried(HostDistance.REMOTE))
//          config.getInt("pool.remote.minSimultaneousRequestsPerConnectionThreshold").map((policy.setMinSimultaneousRequestsPerConnectionThreshold _).curried(HostDistance.REMOTE))
          policy
        })

        // socket.connectTimeout: Milliseconds
        // socket.keepAlive: Boolean
        // socket.readTimeoutMillis: Milliseconds
        // socket.receiveBufferSize: Bytes
        // socket.reuseAddress: Boolean
        // socket.sendBufferSize: Bytes
        // socket.soLingerSecs: Int
        // socket.tcpNoDelay: Boolean
        builder.withSocketOptions({
          val opts = new SocketOptions()
          config.getMilliseconds("socket.connectTimeout").map(_.toInt).map(opts.setConnectTimeoutMillis)
          config.getBoolean("socket.keepAlive").map(opts.setKeepAlive)
          config.getMilliseconds("socket.readTimeoutMillis").map(_.toInt).map(opts.setReadTimeoutMillis)
          config.getBytes("socket.receiveBufferSize").map(_.toInt).map(opts.setReceiveBufferSize)
          config.getBoolean("socket.reuseAddress").map(opts.setReuseAddress)
          config.getBytes("socket.sendBufferSize").map(_.toInt).map(opts.setSendBufferSize)
          config.getInt("socket.soLingerSecs").map(opts.setSoLinger)
          config.getBoolean("socket.tcpNoDelay").map(opts.setTcpNoDelay)
          opts
        })

        // retry.policy: Enum[DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy]
        // retry.log: Boolean
        builder.withRetryPolicy({
          val policy = config.getString(
            "retry.policy",
            Some(Set("DefaultRetryPolicy", "DowngradingConsistencyRetryPolicy", "FallthroughRetryPolicy"))
          ) match {
            case None | Some("DefaultRetryPolicy") => DefaultRetryPolicy.INSTANCE
            case Some("DowngradingConsistencyRetryPolicy") => DowngradingConsistencyRetryPolicy.INSTANCE
            case Some("FallthroughRetryPolicy") => FallthroughRetryPolicy.INSTANCE
            case Some(x) => throw new Exception(s"Unrecognized retry policy: $x")
          }
          config.getBoolean("retry.log") match {
            case Some(true) => new LoggingRetryPolicy(policy)
            case _ => policy
          }
        })

        // loadbalancer: Policy Definition as below:
        //
        // ... .type=DCAwareRoundRobinPolicy
        // ... .localDc: String
        // ... .usedHostsPerRemoteDc: Int
        // ... .allowRemoteDCsForLocalConsistencyLevel: Boolean
        //
        // ... .type=LatencyAwarePolicy
        // ... .exclusionThreshold: Double
        // ... .minimumMeasurements: Int
        // ... .retryPeriod: Duration
        // ... .scale: Duration
        // ... .updateRate: Duration
        // ... .childPolicy: Policy Definition [REQUIRED]
        //
        // ... .type=TokenAwarePolicy
        // ... .childPolicy: Policy Definition [REQUIRED]
        //
        // ... .type=WhiteListPolicy
        // ... .whitelist: List[String] [REQUIRED]
        // ... .childPolicy: Policy Definition [REQUIRED]
        //
        // ... .type=RoundRobinPolicy
        //
        // Default is:
        // loadbalancer = {
        //   type = TokenAwarePolicy
        //   childPolicy = {
        //     type = DCAwareRoundRobinPolicy
        //   }
        // }
        builder.withLoadBalancingPolicy(config.getConfig("loadbalancer").map(loadBalancerFromConfig) getOrElse {
          new policies.TokenAwarePolicy(policies.DCAwareRoundRobinPolicy.builder().build())
        })

        // codecRegistry = "com.foo.SomeCodecRegistry"
        config.getOpt[String]("codecRegistry").map { cr =>
          val clazz = getClass.getClassLoader.loadClass(cr)
          if (!clazz.isAssignableFrom(classOf[CodecRegistry])) {
            throw new Exception(s"codecRegistry must extend ${classOf[CodecRegistry].getCanonicalName}")
          }
          try {
            val constructor = clazz.getDeclaredConstructor()
            if (!constructor.isAccessible) {
              throw new NoSuchMethodException(s"${clazz.getCanonicalName}'s default constructor is not accessible from this location.")
            }
            builder.withCodecRegistry(constructor.newInstance().asInstanceOf[CodecRegistry])

          } catch { case e: NoSuchMethodException =>
            throw new NoSuchMethodException(s"${clazz.getCanonicalName} must provide a public default constructor.").initCause(e)
          }
        }

        builder
          .build()
      }
      lazy val _preparedCache = {
        CacheBuilder.newBuilder()
          .maximumSize(config.getLong("preparedStatementsCacheSize") getOrElse 10000l)
          .removalListener(new RemovalListener[String, PreparedStatement] {

          override def onRemoval(notification: RemovalNotification[String, PreparedStatement]): Unit = {
            logger.warn(s"Removed statement from prepared cache of connection $name.  Cause is ${notification.getCause}\n${notification.getKey}")
          }
        }).build[String, PreparedStatement]()
      }
      lazy val _session = {
        logger.info(s"Creating new session for $name.  Seeing this on startup is just fine.  Seeing this more than once is bad.  Seeing this regularly is extremely bad.")
        config.getString("keyspace") match {
          case Some(ks) => cluster.connect(ks)
          case None => cluster.connect()
        }
      }
      def sessionFactory() = _session
      def cacheFactory(): Cache[String, PreparedStatement] = _preparedCache

      new CassandraClient(cluster, config, tracer, metrics, sessionFactory, cacheFactory)
    }).copy(defaultTracer = tracer, metrics = metrics)
  }

  def loadBalancerFromConfig(config: Configuration): LoadBalancingPolicy = {
    config.getString(
      "type",
      Some(Set("DCAwareRoundRobinPolicy", "LatencyAwarePolicy", "TokenAwarePolicy", "WhiteListPolicy", "RoundRobinPolicy"))
    ) match {
      // ... .type=DCAwareRoundRobinPolicy
      // ... .localDc: String
      // ... .usedHostsPerRemoteDc: Int
      // ... .allowRemoteDCsForLocalConsistencyLevel: Boolean
      case Some("DCAwareRoundRobinPolicy") =>
        val bld = DCAwareRoundRobinPolicy.builder()
        config.getOpt[String]("localDc").map(bld.withLocalDc)
        config.getOpt[Int]("usedHostsPerRemoteDc").map(bld.withUsedHostsPerRemoteDc)
        config.getOpt[Boolean]("allowRemoteDCsForLocalConsistencyLevel") collect {
          case true => bld.allowRemoteDCsForLocalConsistencyLevel()
        }
        bld.build()

      // ... .type=LatencyAwarePolicy
      // ... .exclusionThreshold: Double
      // ... .minimumMeasurements: Int
      // ... .retryPeriod: Duration
      // ... .scale: Duration
      // ... .updateRate: Duration
      // ... .childPolicy: Policy Definition [REQUIRED]
      case Some("LatencyAwarePolicy") =>
        val childPolicy = config.getConfig("childPolicy").map(loadBalancerFromConfig).getOrElse(throw new Exception("LatencyAwarePolicy requires a child policy"))
        val builder = LatencyAwarePolicy.builder(childPolicy)
        config.getDouble("exclusionThreshold").map(builder.withExclusionThreshold)
        config.getInt("minimumMeasurements").map(builder.withMininumMeasurements)
        config.getOpt[FiniteDuration]("retryPeriod").map(d => builder.withRetryPeriod(d.toMillis, TimeUnit.MILLISECONDS))
        config.getOpt[FiniteDuration]("scale").map(d => builder.withScale(d.toMillis, TimeUnit.MILLISECONDS))
        config.getOpt[FiniteDuration]("updateRate").map(d => builder.withUpdateRate(d.toMillis, TimeUnit.MILLISECONDS))
        builder.build()

      // ... .type=TokenAwarePolicy
      // ... .childPolicy: Policy Definition [REQUIRED]
      case Some("TokenAwarePolicy") =>
        val childPolicy = config.getConfig("childPolicy").map(loadBalancerFromConfig).getOrElse(throw new Exception("TokenAwarePolicy requires a child policy"))
        new policies.TokenAwarePolicy(childPolicy)

      // ... .type=WhiteListPolicy
      // ... .whitelist: List[String] [REQUIRED]
      // ... .childPolicy: Policy Definition [REQUIRED]
      case Some("WhiteListPolicy") =>
        val childPolicy = config.getConfig("childPolicy").map(loadBalancerFromConfig).getOrElse(throw new Exception("TokenAwarePolicy requires a child policy"))

        new policies.WhiteListPolicy(
          childPolicy,
          (config.getStringList("loadbalancer.whitelist") match {
            case Some(hosts) => Try(hosts.map(h =>
              InetSocketAddress.createUnresolved(h.split(':')(0), h.split(':')(1).toInt)
            ))
            case None => Failure(new MissingConfigurationException("WhiteListPolicy requires whitelist: List[String]"))
          }).recover { case NonFatal(e) => throw e}.get.asJavaCollection
        )

      // ... .type=RoundRobinPolicy
      case Some("RoundRobinPolicy") => new policies.RoundRobinPolicy()

      case _ => throw new Exception("Load balancer policies require a .type attribute which must be one of \"DCAwareRoundRobinPolicy\", \"LatencyAwarePolicy\", \"TokenAwarePolicy\", \"WhiteListPolicy\", or \"RoundRobinPolicy\"")
    }
  }

}

case class CassandraClient(cluster: Cluster, config: Configuration, defaultTracer: CQLTracer, metrics: Seq[CQLMetrics], sessionFactory: ()=>Session, cacheFactory: ()=>Cache[String, PreparedStatement]) {

  def withMetrics(metricLibs: CQLMetrics*) = copy(metrics = metrics ++ metricLibs)

  protected lazy val _session: CQLSession = CQLSession(sessionFactory(), defaultTracer, metrics, config, cacheFactory())

  implicit def session: CQLSession = _session.copy(metricsTrackers =  metrics)

  def newSession(tracer: CQLTracer = defaultTracer): CQLSession = CQLSession(cluster.newSession(), tracer, metrics, config, cacheFactory())

  //def connect(): Session = cluster.connect()

  def connect(keyspace: String, tracing: Boolean = false, tracer: CQLTracer = defaultTracer): CQLSession = CQLSession(cluster.connect(keyspace), tracer, metrics, config, cacheFactory())

  def getClusterName: String = cluster.getClusterName

  def getMetadata: Metadata = cluster.getMetadata

  def getConfiguration: DSConfiguration = cluster.getConfiguration

  def getMetrics: Metrics = cluster.getMetrics

  def register(listener: StateListener): Cluster = cluster.register(listener)

  def unregister(listener: StateListener): Cluster = cluster.unregister(listener)

  def register(tracker: LatencyTracker): Cluster = cluster.register(tracker)

  def unregister(tracker: LatencyTracker): Cluster = cluster.unregister(tracker)

  def closeAsync(): CloseFuture = cluster.closeAsync()

  def close(): Unit = cluster.close()

  def isClosed: Boolean = cluster.isClosed

  def init(): Cluster = cluster.init()
}
