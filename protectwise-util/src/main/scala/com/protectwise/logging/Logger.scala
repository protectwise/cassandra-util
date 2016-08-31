package com.protectwise.logging

import ch.qos.logback.classic.jul.LevelChangePropagator
import ch.qos.logback.classic.LoggerContext
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.{Logger => SLF4JLogger, MDC}
import scala.reflect._
import scala.util.control.NonFatal

/**
 * Yet another logging facade.
 *
 * Why?
 * - because we want to expose a common interface with simple MDC (not the un-scala-ish MDC of java logging frameworks)
 * - because no logging facade exists that handles Akka actors (they had to do it their own way)
 */
trait Logger {
  def name:String
  def isTraceEnabled:Boolean

  def isDebugEnabled:Boolean

  def isErrorEnabled:Boolean

  def isInfoEnabled:Boolean

  def isWarnEnabled:Boolean

  def trace(msg: => Any, mdc: (String, Any)*):Unit = trace(msg, mdc)
  def debug(msg: => Any, mdc: (String, Any)*):Unit = debug(msg, mdc)
  def info(msg: => Any, mdc: (String, Any)*):Unit = info(msg, mdc)
  def warn(msg: => Any, mdc: (String, Any)*):Unit = warn(msg, mdc)
  def warn(msg: => Any, e: => Throwable, mdc: (String, Any)*):Unit = warn(msg, e, mdc)
  def error(msg: => Any, mdc: (String, Any)*):Unit = error(msg, mdc)
  def error(msg: => Any, e: => Throwable, mdc: (String, Any)*):Unit = error(msg, e, mdc)

  def trace(message: => Any, mdc: => Iterable[(String, Any)])
  def debug(message: => Any, mdc: => Iterable[(String, Any)])
  def debug(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)])
  def info(message: => Any, mdc: => Iterable[(String, Any)])
  def info(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)])
  def warn(message: => Any, mdc: => Iterable[(String, Any)])
  def warn(message: => Any, e: => Throwable, mdc: => Iterable[(String, Any)])
  def error(message: => Any, mdc: Iterable[(String, Any)])
  def error(message: => Any, e: => Throwable, mdc: Iterable[(String, Any)])
}

abstract class AbstractLogger extends Logger {
  /** Proxies for underlying implementation */
  protected def writeTrace(msg: => Any):Unit
  protected def writeDebug(msg: => Any):Unit
  protected def writeInfo(msg: => Any):Unit
  protected def writeWarn(msg: => Any):Unit
  protected def writeWarn(msg: => Any, e: => Throwable):Unit
  protected def writeError(msg: => Any):Unit
  protected def writeError(msg: => Any, e: => Throwable):Unit

  def trace(message: => Any, mdc: => Iterable[(String, Any)])  =
    if(isTraceEnabled) logWithMDC(mdc, writeTrace(message))

  def debug(message: => Any, mdc: => Iterable[(String, Any)]) =
    if(isDebugEnabled) logWithMDC(mdc, writeDebug(message))

  def debug(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)]) =
    if (isDebugEnabled) logWithMDC(mdc, writeDebug(message))

  def info(message: => Any, mdc: => Iterable[(String, Any)]) =
    if(isInfoEnabled) logWithMDC(mdc, writeInfo(message))

  def info(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)]) =
    if(isInfoEnabled) logWithMDC(mdc, writeInfo(message))

  def warn(message: => Any, mdc: => Iterable[(String, Any)]) =
    if(isWarnEnabled) logWithMDC(mdc, writeWarn(message))

  def warn(message: => Any, e: => Throwable, mdc: => Iterable[(String, Any)]) =
    if(isWarnEnabled) logWithMDC(mdc, writeWarn(message, e))

  def error(message: => Any, mdc: Iterable[(String, Any)]) =
    if(isErrorEnabled) logWithMDC(mdc, writeError(message))

  def error(message: => Any, e: => Throwable, mdc: Iterable[(String, Any)]) =
    if(isErrorEnabled) logWithMDC(mdc, writeError(message, e))

  protected[protectwise] def logWithMDC(mdc:Iterable[(String, Any)], logger: => Unit):Unit
}

object Logger {
  if (!SLF4JBridgeHandler.isInstalled()) {
    // For performance reasons with j.u.l., add a LevelChangePropagator as a listener to Loggers through
    // the LoggerFactory. This operation avoids unnecessary conversions.
    // See http://www.slf4j.org/api/org/slf4j/bridge/SLF4JBridgeHandler.html
    try {
      // This can throw if the logger factory is not
      // a LoggerContext.
      val context = org.slf4j.LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
      val levelChangePropagator = new LevelChangePropagator()
      levelChangePropagator.setContext(context)
      levelChangePropagator.setResetJUL(true)
      context.addListener(levelChangePropagator)
    } catch {
      case NonFatal(ex) =>
    }
    // Install the bridge between j.u.l. and SLF4J.
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }

  def apply(name: String, defaultMdc: Iterable[(String, Any)] = Iterable.empty[(String, Any)]): Logger =
    new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(name), defaultMdc)

  def apply(cls: Class[_]): Logger = {
    apply(cls.getName, Iterable.empty[(String, Any)])
  }

  def apply(cls: Class[_], defaultMdc: Iterable[(String, Any)]): Logger = {
    apply(cls.getName, defaultMdc)
  }

  def apply[C: ClassTag](defaultMdc: Iterable[(String, Any)]): Logger = {
    apply(classTag[C].runtimeClass.getName, defaultMdc)
  }

  def apply[C: ClassTag](): Logger = {
    apply(classTag[C].runtimeClass.getName, Iterable.empty[(String, Any)])
  }
}

private[logging] class Slf4jLogger(val logger: SLF4JLogger, val defaultMdc: Iterable[(String, Any)]) extends AbstractLogger {
  override def name: String = logger.getName

  def isTraceEnabled = logger.isTraceEnabled

  def isDebugEnabled = logger.isDebugEnabled

  def isErrorEnabled = logger.isErrorEnabled

  def isInfoEnabled = logger.isInfoEnabled

  def isWarnEnabled = logger.isWarnEnabled

  protected def writeTrace(msg: => Any):Unit = if (isTraceEnabled) logger.trace(msg.toString)
  protected def writeDebug(msg: => Any):Unit = if (isDebugEnabled) logger.debug(msg.toString)
  protected def writeInfo(msg: => Any):Unit = if (isInfoEnabled) logger.info(msg.toString)
  protected def writeWarn(msg: => Any):Unit = if (isWarnEnabled) {
    logger.warn(msg.toString)
  }
  protected def writeWarn(msg: => Any, e: => Throwable):Unit = if (isWarnEnabled) {
    logger.warn(msg.toString, e)
  }
  protected def writeError(msg: => Any):Unit = if(isErrorEnabled) {
    logger.error(msg.toString)
  }
  protected def writeError(msg: => Any, e: => Throwable):Unit = if(isErrorEnabled) {
    logger.error(msg.toString, e)
  }

  protected[protectwise] def logWithMDC(mdc: Iterable[(String, Any)], logger: => Unit): Unit = {
    val fullMdc = mdc ++ defaultMdc
    fullMdc.foreach {
      case (k, v) if v != null => MDC.put(k, v.toString)
      case _ => //do nothing
    }
    logger
    MDC.clear()
  }
}

class AgentLogger(logger:Logger) extends Logger {
  import MDCKeys._
  override def name: String = logger.name

  override def isTraceEnabled: Boolean = logger.isTraceEnabled
  override def isDebugEnabled: Boolean = logger.isDebugEnabled
  override def isInfoEnabled: Boolean = logger.isInfoEnabled
  override def isErrorEnabled: Boolean = logger.isErrorEnabled
  override def isWarnEnabled: Boolean = logger.isWarnEnabled

  override def trace(message: => Any, mdc: => Iterable[(String, Any)]): Unit = logger.trace(message, mdc)
  override def debug(message: => Any, mdc: => Iterable[(String, Any)]): Unit = logger.debug(message, mdc)
  override def debug(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)]): Unit = logger.debug(message, t, mdc)
  override def info(message: => Any, mdc: => Iterable[(String, Any)]): Unit = logger.info(message, mdc)
  override def info(message: => Any, t: => Throwable, mdc: => Iterable[(String, Any)]): Unit = logger.info(message, t, mdc)
  override def warn(message: => Any, mdc: => Iterable[(String, Any)]): Unit = logger.warn(message, mdc)
  override def warn(message: => Any, e: => Throwable, mdc: => Iterable[(String, Any)]): Unit = logger.warn(message, e, mdc)
  override def error(message: => Any, mdc: Iterable[(String, Any)]): Unit = logger.error(message, mdc)
  override def error(message: => Any, e: => Throwable, mdc: Iterable[(String, Any)]): Unit = logger.error(message, e, mdc)

  def trace(agentId: Long, msg: => Any): Unit = trace(msg, (agentIdKey -> agentId))
  def debug(agentId: Long, msg: => Any): Unit = debug(msg, (agentIdKey -> agentId))
  def info(agentId: Long, msg: => Any): Unit = info(msg, Map(agentIdKey -> agentId))
  def warn(agentId: Long, msg: => Any): Unit = info(msg, Map(agentIdKey -> agentId))
  def error(agentId: Long, msg: => Any): Unit = error(msg, Map(agentIdKey -> agentId))

  def trace(agentId: Long, message: => Any, mdc: => Iterable[(String, Any)]) =
    logger.trace(message, Map(agentIdKey -> agentId) ++ mdc)
  def debug(agentId: Long, message: => String, mdc: => Iterable[(String, Any)]) =
    logger.debug(message, Map(agentIdKey -> agentId) ++ mdc)
  def info(agentId: Long, message: => String, mdc: => Iterable[(String, Any)]) =
    logger.info(message, Map(agentIdKey -> agentId) ++ mdc)
  def warn(agentId: Long, message: => String, mdc: => Iterable[(String, Any)]) =
    logger.warn(message, Map(agentIdKey -> agentId) ++ mdc)
  def warn(agentId: Long, message: => String, e: => Throwable, mdc: => Iterable[(String, Any)]) =
    logger.warn(message, e, Map(agentIdKey -> agentId) ++ mdc)
  def error(agentId: Long, message: => String, mdc: Iterable[(String, Any)]) =
    logger.error(message, Map(agentIdKey -> agentId) ++ mdc)
  def error(agentId: Long, message: => String, e: => Throwable, mdc: => Iterable[(String, Any)] = Map.empty) =
    logger.error(message, e, Map(agentIdKey -> mdc) ++ mdc)
}

