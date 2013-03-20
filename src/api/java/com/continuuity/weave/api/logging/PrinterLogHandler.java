package com.continuuity.weave.api.logging;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import java.util.TimeZone;

/**
 * A {@link LogHandler} that prints the {@link LogEntry} through a {@link PrintWriter}.
 */
public final class PrinterLogHandler implements LogHandler {

  private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS'Z'");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      return format;
    }
  };

  private final PrintWriter writer;
  private final Formatter formatter;

  /**
   * Creates a {@link PrinterLogHandler} which has {@link LogEntry} written to the given {@link PrintWriter}.
   * @param writer The write that log entries will write to.
   */
  public PrinterLogHandler(PrintWriter writer) {
    this.writer = writer;
    this.formatter = new Formatter(writer);
  }

  @Override
  public void onLog(LogEntry logEntry) {
    String utc = timestampToUTC(logEntry.getTimestamp());

    formatter.format("%s %-5s [%s] [%s] %s:%s - %s\n",
                     utc,
                     logEntry.getLogLevel().name(),
                     logEntry.getHost().getCanonicalHostName(),
                     logEntry.getThreadName(),
                     logEntry.getSourceClassName(),
                     logEntry.getSourceMethodName(),
                     logEntry.getMessage());
    formatter.flush();

    Throwable throwable = logEntry.getThrowable();
    if (throwable != null) {
      throwable.printStackTrace(writer);
      writer.flush();
    }
  }

  private String timestampToUTC(long timestamp) {
    return DATE_FORMAT.get().format(new Date(timestamp));
  }
}
