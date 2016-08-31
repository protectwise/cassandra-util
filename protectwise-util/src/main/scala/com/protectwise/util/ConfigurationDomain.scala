package com.protectwise.util

import com.protectwise.util.ConfigurationDomain.MissingConfigurationException

import scala.reflect.ClassTag

/**
  * Created by eric on 6/29/16.
  */
object ConfigurationDomain {
  lazy val config = Configuration.load()

  class MissingConfigurationException(rootPath: String, name: String) extends Exception(s"Missing configuration $name at path $rootPath")
}

trait ConfigurationDomain {
  /**
    * The name of the config section that this ConfigurationDomain belongs to
    *
    * Using a 'def' so that it can be override in implementing classes
    *
    * @return
    */
  protected def configurationDomain: String = getClass.getName

  lazy val configuration = ConfigurationDomain.config.getConfiguration(configurationDomain)

  def conf[T: ClassTag](name: String):T = configuration.get[T](name, Set.empty[T])

  def conf[T: ClassTag](name: String, validValues:Set[T]):T =
    configuration.getOpt[T](name, validValues).getOrElse(throw new MissingConfigurationException(configurationDomain, name))

  def conf[T: ClassTag](name: String, default: => T):T = configuration.get[T](name, default, Set.empty[T])

  def conf[T: ClassTag](name: String, default: => T, validValues:Set[T]):T =
    configuration.get[T](name, default, validValues)

  def confOpt[T: ClassTag](name: String, validValues: Set[T] = Set.empty[T]): Option[T] =
    configuration.getOpt[T](name, validValues)
}

/**
  * More concise version of Configuration Domain
  */
trait Configurable extends ConfigurationDomain {
  override def configurationDomain = configName
  protected def configName:String
}
