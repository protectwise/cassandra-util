package com.protectwise.logging

object Logging {
  def apply(clazz: Class[_], defaultMdcKeys: Iterable[(String, Any)] = Iterable.empty[(String, Any)]): Logging = new Logging {
    override def _logger = Logger(clazz)
  }
}

/**
 * Mix-in trait to gain access to a Logger
 */
trait Logging extends MDCKeys {
  //allow for wrapping the internal logger instance in sub-classes
  protected[this] def _logger = Logger(getClass(), defaultMdc)

  //this is the one that everyone should access
  @transient
  protected[this] lazy val logger = _logger

  /**
   * Stackable list of defaultMdc data to populate for a given logger
   *
   * @return
   */
  protected def defaultMdc = Seq.empty[(String, Any)]
}

/**
 * Mix in to allow for setting the agentId in the MDC context.
 *
 * Also exposes a wrapped version of Logger that has special methods to
 * log out the different Agent ID MDC values on a per call basis
 */
trait AgentLogging extends Logging {
  /**
   * An AgentLogger instance
   *
   * import logger._ to acheive the old AgentLogging functionality
   */
  @transient
  override protected[this] lazy val logger = new AgentLogger(_logger)
}