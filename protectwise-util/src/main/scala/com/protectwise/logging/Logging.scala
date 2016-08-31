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
