package com.continuuity.weave.api.logging;

import java.net.InetAddress;
import java.util.logging.Level;

/**
 *
 */
public interface LogEntry {

  InetAddress getHost();

  long getTimestamp();

  Level getLogLevel();

  String getLoggerName();

  String getSourceClassName();

  String getSourceMethodName();

  String getMessage();

  Throwable getThrowable();
}
