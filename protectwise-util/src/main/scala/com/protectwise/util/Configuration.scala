/*
 * Copyright 2014 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.util

import java.util.concurrent.TimeUnit

import com.typesafe.config._

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect._
import scala.util.control.NonFatal

/**
  * This object provides a set of operations to create `Configuration` values.
  *
  * For example, to load a `Configuration` in a running application:
  * {{{
  * val config = Configuration.load()
  * val foo = config.getString("foo").getOrElse("boo")
  * }}}
  *
  * The underlying implementation is provided by https://github.com/typesafehub/config.
  */
object Configuration {

  class ConfigurationException(typeStr: String, message: String, exceptionOpt: Option[Throwable]) extends Throwable {
    val exception = exceptionOpt.getOrElse("None")
    throw new Exception(s"Type: ${typeStr} Message: ${message} Trace: ${exception} ")
  }

  class MissingConfigurationException(name: String)
    extends Exception(s"Missing configuration $name")

  private[this] lazy val dontAllowMissingConfigOptions = ConfigParseOptions.defaults().setAllowMissing(false)

  private[this] lazy val dontAllowMissingConfig = ConfigFactory.load(dontAllowMissingConfigOptions)

  /**
    * Loads a new `Configuration` either from the classpath or from
    * `conf/application.conf` depending on the application's Mode.
    *
    * The provided mode is used if the application is not ready
    * yet, just like when calling this method from `play.api.Application`.
    *
    * Defaults to Mode.Dev
    *
    * @return a `Configuration` instance
    */
  def load() = {
    try {
      Configuration(ConfigFactory.load)
    } catch {
      case e: ConfigException => throw configError(e.origin, e.getMessage, Some(e))
      case e: Throwable => throw e
    }
  }

  /**
    * Returns an empty Configuration object.
    */
  def empty = Configuration(ConfigFactory.empty())

  /**
    * Create a ConfigFactory object from the data passed as a Map.
    */
  def from(data: Map[String, Any]) = {

    def asJavaRecursively[A](data: Map[A, Any]): Map[A, Any] = {
      data.mapValues { value =>
        value match {
          case v: Map[_, _] => asJavaRecursively(v).asJava
          case v: Iterable[_] => v.asJava
          case v => v
        }
      }
    }

    Configuration(ConfigFactory.parseMap(asJavaRecursively[String](data).asJava))
  }

  private def configError(origin: ConfigOrigin, message: String, e: Option[Throwable] = None): Throwable = {
    //import scalax.io.JavaConverters._
    new ConfigurationException("Configuration error", message, e)
  }

  private def missingError(path: String) = {
    new MissingConfigurationException(path)
  }
}

/**
  * A full configuration set.
  *
  * The underlying implementation is provided by https://github.com/typesafehub/config.
  *
  * @param underlying the underlying Config implementation
  */
case class Configuration(underlying: Config) {

  /**
    * Merge 2 configurations.
    */
  def ++(other: Configuration): Configuration = {
    Configuration(other.underlying.withFallback(underlying))
  }

  /**
    * Merge 2 configurations.
    */
  def ++(map: Map[String, Any]): Configuration = {
    this ++ Configuration.from(map)
  }

  def +(kv: (String, Any)): Configuration = {
    this ++ Configuration.from(Map(kv))
  }

  /**
    * Read a value from the underlying implementation,
    * catching Errors and wrapping it in an Option value.
    */
  private def readValue[T](path: String, v: => T): Option[T] = {
    try {
      Option(v)
    } catch {
      case e: ConfigException.Missing => None
      case NonFatal(e) => throw reportError(path, e.getMessage, Some(e))
    }
  }

  def get[T: ClassTag](name: String):T = get[T](name, Set.empty[T])

  def get[T: ClassTag](name: String, validValues:Set[T]):T = getOpt[T](name, validValues).getOrElse(throw reportMissing(name))

  def get[T: ClassTag](name: String, default: => T):T = get[T](name, default, Set.empty[T])

  def get[T: ClassTag](name: String, default: => T, validValues:Set[T]):T =
    getOpt[T](name, validValues).getOrElse(default)

  def getOpt[T: ClassTag](name: String, validValues: Set[T] = Set.empty[T]): Option[T] = synchronized {
    val maybeValue = (classTag[T].runtimeClass match {
      case s if s == classOf[String] => getString(name)
      case i if i == classOf[Int] || i == classOf[java.lang.Integer] => getInt(name)
      case l if l == classOf[Long] || l == classOf[java.lang.Long] => getLong(name)
      case d if d == classOf[Double] || d == classOf[java.lang.Double] => getDouble(name)
      case b if b == classOf[Float] || b == classOf[java.lang.Float] => getFloat(name)
      case b if b == classOf[Boolean] || b == classOf[java.lang.Boolean] => getBoolean(name)
      case f if f == classOf[FiniteDuration] => getNanoseconds(name).map(Duration.fromNanos)
      case d if d == classOf[Duration] =>
        getString(name).collect {
          case s if s.toLowerCase().startsWith("inf") => Duration.Inf
        } orElse getNanoseconds(name).map(Duration.fromNanos)
      case c if c == classOf[Configuration] => getConfig(name)
    }).asInstanceOf[Option[T]]
    maybeValue.map {
      case valid if validValues.contains(valid) => valid
      case valid if validValues.isEmpty => valid
      case _ =>
        throw reportError(name, "Incorrect value, one of " + (validValues.mkString(",")) + " was expected.")
    }
  }

  def getConfiguration(key: String, default: Configuration = Configuration.empty): Configuration = {
    getConfig(key).getOrElse(default)
  }

  /**
    * Retrieves a configuration value as a `String`.
    *
    * This method supports an optional set of valid values:
    * {{{
    * val config = Configuration.load()
    * val mode = config.getString("engine.mode", Some(Set("dev","prod")))
    * }}}
    *
    * A configuration error will be thrown if the configuration value does not match any of the required values.
    *
    * @param path the configuration key, relative to configuration root key
    * @param validValues valid values for this configuration
    * @return a configuration value
    */
  def getString(path: String, validValues: Option[Set[String]] = None): Option[String] = readValue(path,
    underlying.getString(path)).map { value =>
    validValues match {
      case Some(values) if values.contains(value) => value
      case Some(values) if values.isEmpty => value
      case Some(values) => throw reportError(path,
        "Incorrect value, one of " + (values.reduceLeft(_ + ", " + _)) + " was expected.")
      case None => value
    }
  }

  /**
    * Retrieves a configuration value as an `Int`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val poolSize = configuration.getInt("engine.pool.size")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Int`.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getInt(path: String): Option[Int] = readValue(path, underlying.getInt(path))

  /**
    * Retrieves a configuration value as a `Boolean`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val isEnabled = configuration.getBoolean("engine.isEnabled")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Boolean`.
    *
    * Authorized vales are `yes/no or true/false.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getBoolean(path: String): Option[Boolean] = readValue(path, underlying.getBoolean(path))

  /**
    * Retrieves a configuration value as `Milliseconds`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val timeout = configuration.getMilliseconds("engine.timeout")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.timeout = 1 second
    * }}}
    */
  def getMilliseconds(path: String): Option[Long] = readValue(path, underlying.getDuration(path, TimeUnit.MILLISECONDS))

  /**
    * Retrieves a configuration value as `Nanoseconds`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val timeout = configuration.getNanoseconds("engine.timeout")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.timeout = 1 second
    * }}}
    */
  def getNanoseconds(path: String): Option[Long] = readValue(path, underlying.getDuration(path, TimeUnit.NANOSECONDS))

  /**
    * Retrieves a configuration value as `Bytes`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSize = configuration.getString("engine.maxSize")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSize = 512k
    * }}}
    */
  def getBytes(path: String): Option[Long] = readValue(path, underlying.getBytes(path))

  /**
    * Retrieves a sub-configuration, i.e. a configuration instance containing all keys starting with a given prefix.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val engineConfig = configuration.getSub("engine")
    * }}}
    *
    * The root key of this new configuration will be ‘engine’, and you can access any sub-keys relatively.
    *
    * @param path the root prefix for this sub-configuration
    * @return a new configuration
    */
  def getConfig(path: String): Option[Configuration] = readValue(path, underlying.getConfig(path)).map(Configuration(_))

  /**
    * Retrieves a configuration value as a `Double`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val population = configuration.getDouble("world.population")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Double`.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getDouble(path: String): Option[Double] = readValue(path, underlying.getDouble(path))

  /**
    * Retrieves a configuration value as a `Float`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val population = configuration.getDouble("world.population")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Float`.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getFloat(path: String): Option[Float] = readValue(path, underlying.getDouble(path).toFloat)

  /**
    * Retrieves a configuration value as a `Long`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val duration = configuration.getLong("timeout.duration")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Long`.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getLong(path: String): Option[Long] = readValue(path, underlying.getLong(path))

  /**
    * Retrieves a configuration value as a `Number`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val counter = configuration.getNumber("foo.counter")
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Number`.
    *
    * @param path the configuration key, relative to the configuration root key
    * @return a configuration value
    */
  def getNumber(path: String): Option[Number] = readValue(path, underlying.getNumber(path))

  /**
    * Retrieves a configuration value as a List of `Boolean`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val switches = configuration.getBooleanList("board.switches")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * board.switches = [true, true, false]
    * }}}
    *
    * A configuration error will be thrown if the configuration value is not a valid `Boolean`.
    * Authorized vales are `yes/no or true/false.
    */
  def getBooleanList(path: String): Option[List[Boolean]] = readValue(path,
    underlying.getBooleanList(path).asScala.toList.map(_.booleanValue()))

  /**
    * Retrieves a configuration value as a List of `Bytes`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getBytesList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = [512k, 256k, 256k]
    * }}}
    */
  def getBytesList(path: String): Option[List[Long]] = readValue(path,
    underlying.getBytesList(path).asScala.toList.map(_.toLong))

  /**
    * Retrieves a List of sub-configurations, i.e. a configuration instance for each key that matches the path.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val engineConfigs = configuration.getConfigList("engine")
    * }}}
    *
    * The root key of this new configuration will be "engine", and you can access any sub-keys relatively.
    */
  def getConfigList(path: String): Option[List[Configuration]] = readValue[java.util.List[_ <: Config]](path,
    underlying.getConfigList(path)).map { configs => configs.asScala.map(Configuration(_)).toList}

  /**
    * Retrieves a configuration value as a List of `Double`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getDoubleList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = [5.0, 3.34, 2.6]
    * }}}
    */
  def getDoubleList(path: String): Option[List[Double]] = readValue(path,
    underlying.getDoubleList(path).asScala.toList.map(_.toDouble))

  /**
    * Retrieves a configuration value as a List of `Integer`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getIntList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = [100, 500, 2]
    * }}}
    */
  def getIntList(path: String): Option[List[Integer]] = readValue(path, underlying.getIntList(path).asScala.toList)

  /**
    * Gets a list value (with any element type) as a ConfigList, which implements java.util.List<ConfigValue>.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = ["foo", "bar"]
    * }}}
    */
  def getList(path: String): Option[ConfigList] = readValue(path, underlying.getList(path))

  /**
    * Retrieves a configuration value as a List of `Long`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getLongList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = [10000000000000, 500, 2000]
    * }}}
    */
  def getLongList(path: String): Option[List[Long]] =
    readValue(path, underlying.getLongList(path).asScala.toList.map(_.toLong))

  /**
    * Retrieves a configuration value as List of `Milliseconds`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val timeouts = configuration.getMillisecondsList("engine.timeouts")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.timeouts = [1 second, 1 second]
    * }}}
    */
  def getMillisecondsList(path: String): Option[List[Long]] = readValue(path,
    underlying.getDurationList(path, TimeUnit.MILLISECONDS).asScala.toList.map(_.toLong))

  /**
    * Retrieves a configuration value as List of `Nanoseconds`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val timeouts = configuration.getNanosecondsList("engine.timeouts")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.timeouts = [1 second, 1 second]
    * }}}
    */
  def getNanosecondsList(path: String): Option[List[Long]] = readValue(path,
    underlying.getDurationList(path, TimeUnit.NANOSECONDS).asScala.toList.map(_.toLong))

  /**
    * Retrieves a configuration value as a List of `Number`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val maxSizes = configuration.getNumberList("engine.maxSizes")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * engine.maxSizes = [50, 500, 5000]
    * }}}
    */
  def getNumberList(path: String): Option[List[Number]] = readValue(path,
    underlying.getNumberList(path).asScala.toList)

  /**
    * Retrieves a configuration value as a List of `String`.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val names = configuration.getStringList("names")
    * }}}
    *
    * The configuration must be provided as:
    *
    * {{{
    * names = ["Jim", "Bob", "Steve"]
    * }}}
    */
  def getStringList(path: String): Option[List[String]] = readValue(path,
    underlying.getStringList(path).asScala.toList)

  /**
    * Returns available keys.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val keys = configuration.keys
    * }}}
    *
    * @return the set of keys available in this configuration
    */
  def keys: Set[String] = underlying.entrySet.asScala.map(_.getKey).toSet

  /**
    * Returns sub-keys.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * val subKeys = configuration.subKeys
    * }}}
    *
    * @return the set of direct sub-keys available in this configuration
    */
  def subKeys: Set[String] = underlying.root().keySet().asScala.toSet

  /**
    * Returns every path as a set of key to value pairs, by recursively iterating through the
    * config objects.
    */
  def entrySet: Set[(String, ConfigValue)] = underlying.entrySet().asScala.map(e => e.getKey -> e.getValue).toSet

  /**
    * Creates a configuration error for a specific configuration key.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * throw configuration.reportError("engine.connectionUrl", "Cannot connect!")
    * }}}
    *
    * @param path the configuration key, related to this error
    * @param message the error message
    * @param e the related exception
    * @return a configuration exception
    */
  def reportError(path: String, message: String, e: Option[Throwable] = None): Throwable = {
    Configuration
      .configError(if (underlying.hasPath(path)) underlying.getValue(path).origin else underlying.root.origin, message,
        e)
  }

  def reportMissing(path: String) = {
    Configuration.missingError(path)
  }

  /**
    * Creates a configuration error for this configuration.
    *
    * For example:
    * {{{
    * val configuration = Configuration.load()
    * throw configuration.globalError("Missing configuration key: [yop.url]")
    * }}}
    *
    * @param message the error message
    * @param e the related exception
    * @return a configuration exception
    */
  def globalError(message: String, e: Option[Throwable] = None): Throwable = {
    Configuration.configError(underlying.root.origin, message, e)
  }

  def render(options: ConfigRenderOptions = ConfigRenderOptions.defaults()) = underlying.root().render(options)

  override def toString: String = s"Configuration(${render(ConfigRenderOptions.concise())})"

  override def equals(obj: scala.Any): Boolean = obj match {
    case conf:Configuration => conf.underlying == underlying
    case _ => false
  }
}
