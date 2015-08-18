package org.realityforge.dbdiff;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

final class CollectorFormatter
  extends Formatter
{
  @Override
  public String format( final LogRecord logRecord )
  {
    return logRecord.getMessage() + "\n";
  }
}