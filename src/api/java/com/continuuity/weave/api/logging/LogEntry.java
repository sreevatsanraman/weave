package com.continuuity.weave.api.logging;

import java.net.InetAddress;
import java.util.logging.Level;

/**
 *
 */
public interface LogEntry {

  enum Level {
    FATAL,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE
  }

  InetAddress getHost();

  long getTimestamp();

  Level getLogLevel();

  String getSourceClassName();

  String getSourceMethodName();

  String getThreadName();

  String getMessage();

  Throwable getThrowable();
}
