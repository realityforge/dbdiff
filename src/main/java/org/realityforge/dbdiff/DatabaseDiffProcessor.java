package org.realityforge.dbdiff;

import java.sql.Connection;

public final class DatabaseDiffProcessor
{
  private final Connection _connection1;
  private final Connection _connection2;
  private final String _dialect;
  private final DatabaseDifferenceHandler _differenceHandler;

  public DatabaseDiffProcessor( final Connection connection1,
                                final Connection connection2,
                                final String dialect,
                                final DatabaseDifferenceHandler differenceHandler )
  {
    _connection1 = connection1;
    _connection2 = connection2;
    _dialect = dialect;
    _differenceHandler = differenceHandler;
  }

  public void performDiff()
    throws Exception
  {
  }
}
