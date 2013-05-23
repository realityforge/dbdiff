package org.realityforge.dbdiff;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;
import org.postgresql.Driver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import static org.testng.Assert.*;

public abstract class AbstractDatabaseDiffTest
{
  protected abstract Dialect getDialect();

  protected abstract Driver getDriver();

  protected abstract String getDatabase1();

  protected abstract String getDatabase2();

  protected final void assertMatch( final String schema,
                                    final String ddl1,
                                    final String ddl2 )
    throws Exception
  {
    diff( schema, ddl1, ddl2, true );
  }

  protected final void assertNotMatch( final String schema,
                                       final String ddl1,
                                       final String ddl2 )
    throws Exception
  {
    diff( schema, ddl1, ddl2, false );
  }

  private void diff( final String schema,
                     final String ddl1,
                     final String ddl2,
                     final boolean shouldMatch )
    throws Exception
  {
    setupDatabases();
    final DatabaseDiff dd = newDatabaseDiff();
    dd.setLogger( newLogger() );
    dd.getSchemas().add( schema );

    executeSQL( ddl1, getDatabase1() );
    executeSQL( ddl2, getDatabase2() );

    assertEquals( dd.diff(), !shouldMatch, "Does diff match expected" );
    tearDownDatabases();
  }

  protected abstract void setupDatabases()
    throws Exception;

  protected abstract void tearDownDatabases()
    throws Exception;

  private DatabaseDiff newDatabaseDiff()
  {
    final DatabaseDiff dd = new DatabaseDiff();
    dd.setDialect( getDialect() );
    dd.setDriver( getDriver() );
    dd.setDatabase1( getDatabase1() );
    dd.setDatabase2( getDatabase2() );
    return dd;
  }

  private Logger newLogger()
  {
    final Logger logger = Logger.getAnonymousLogger();
    logger.setUseParentHandlers( false );
    final ConsoleHandler handler = new ConsoleHandler();
    handler.setFormatter( new RawFormatter() );
    logger.addHandler( handler );
    return logger;
  }

  protected final void executeSQL( final String sql, final String database )
    throws SQLException
  {
    final Connection connection1 = getDriver().connect( database, new Properties() );
    connection1.createStatement().execute( sql );
    connection1.close();
  }

  protected final String join( final char separator, final String... commands )
  {
    final StringBuilder sb = new StringBuilder();
    for ( final String command : commands )
    {
      if ( 0 != sb.length() )
      {
        sb.append( separator );
      }
      sb.append( command );
    }
    return sb.toString();
  }

  protected final String s( final String... commands )
  {
    return join( ';', commands );
  }
}
