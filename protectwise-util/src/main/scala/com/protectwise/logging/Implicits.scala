package com.protectwise.logging

import java.util.logging.{Level, LogRecord}

/**
 * Created by jho on 4/3/15.
 */
object Implicits {
  implicit def loggerToJUL(logger: Logger):java.util.logging.Logger = {
    new java.util.logging.Logger(logger.name, null) {
      // Intercept calls to the heart of the logging operation to ship them off to our logger instead.
      override def log(record: LogRecord): Unit = {
        val theFilter = getFilter

        if (theFilter != null && !theFilter.isLoggable(record)) {
          return
        } else {
          def msg = s"${record.getLoggerName }: ${record.getMessage.format(record.getParameters) }}"

          record.getLevel.intValue() match {
            case v if v >=
              Level.OFF.intValue() => // Int.MaxValue - probably won't happen practically, who would log at OFF level?
            case v if v >= Level.SEVERE.intValue() => // 1000
              val t = record.getThrown
              if (t != null) logger.error(msg, t)
              else logger.error(msg)
            case v if v >= Level.WARNING.intValue() => // 900
              val t = record.getThrown
              if (t != null) logger.warn(msg, t)
              else logger.warn(msg)
            case v if v >= Level.CONFIG.intValue() => // 700 - This also includes INFO(800) level
              logger.info(msg)
            case v if v >= Level.FINER.intValue() => // 400 - This also includes FINE(500) level
              logger.debug(msg)
            case v if v >= Level.ALL.intValue() => // Int.MinValue - this also includes FINEST(300) level
              logger.trace(msg)
          }

        }
      }
    }
  }
}
