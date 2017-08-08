package org.realityforge.dbdiff;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.postgresql.Driver;
import static org.testng.Assert.*;

public abstract class AbstractDatabaseDiffTest
{
  private final boolean _emitDiff = System.getProperty( "test.emit.diff", "false" ).equalsIgnoreCase( "true" );
  private ArrayList<String> _output = new ArrayList<>();

  final class CollectorFormatter
    extends Formatter
  {
    @Override
    public String format( final LogRecord logRecord )
    {
      _output.add( logRecord.getMessage() );
      if ( _emitDiff )
      {
        return logRecord.getMessage() + "\n";
      }
      else
      {
        return "";
      }
    }
  }

  protected abstract Dialect getDialect();

  protected abstract Driver getDriver();

  protected abstract String getDatabase1();

  protected abstract String getDatabase2();

  protected Properties getDbProperties()
  {
    return new Properties();
  }

  protected final void assertDiffOutput( final String regex )
  {
    final Pattern pattern = Pattern.compile( regex );
    for ( final String line : _output )
    {
      if ( pattern.matcher( line ).matches() )
      {
        return;
      }
    }
    System.out.println( "Failed to match output " + regex + " in:" );
    for ( final String line : _output )
    {
      System.out.println( line );
    }
    fail( "Failed to match output " + regex + " in:" );
  }

  protected final void assertDiffOutput( final String... regexs )
  {
    int line = 0;
    int regexIndex = 0;
    boolean matched = true;
    while ( regexIndex < regexs.length )
    {
      final Pattern pattern = Pattern.compile( regexs[ regexIndex ] );
      matched = false;
      while ( line < _output.size() )
      {
        final String text = _output.get( line );
        line++;
        if ( pattern.matcher( text ).matches() )
        {
          matched = true;
          break;
        }
      }
      if ( !matched )
      {
        break;
      }
      regexIndex++;
    }
    if( matched )
    {
      return;
    }
    final String errorMessage =
      "Failed to match output " + Arrays.asList( regexs ) + " at " + regexs[regexIndex] + " in:";
    System.out.println( errorMessage );
    for ( final String l : _output )
    {
      System.out.println( l );
    }
    fail( errorMessage );
  }
  protected final void assertMatch( final String schema,
                                    final String ddl1,
                                    final String ddl2 )
    throws Exception
  {
    purgeDiffOutput();
    assertSameDDLMatches( schema, ddl1 );
    assertSameDDLMatches( schema, ddl2 );
    diff( schema, ddl1, ddl2, true );
  }

  protected final void assertNotMatch( final String schema,
                                       final String ddl1,
                                       final String ddl2 )
    throws Exception
  {
    purgeDiffOutput();
    assertSameDDLMatches( schema, ddl1 );
    assertSameDDLMatches( schema, ddl2 );
    diff( schema, ddl1, ddl2, false );
  }

  private void assertSameDDLMatches( final String schema, final String ddl1 )
    throws Exception
  {
    diff( schema, ddl1, ddl1, true );
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

  private void purgeDiffOutput()
  {
    _output.clear();
  }

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
    handler.setFormatter( new CollectorFormatter() );
    logger.addHandler( handler );
    return logger;
  }

  protected final void executeSQL( final String sql, final String database )
    throws SQLException
  {
    final Connection connection1 = getDriver().connect( database, getDbProperties() );
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

  protected final String[] quote( final String before,
                                  final String after,
                                  final String... elements )
  {
    final String[] results = new String[ elements.length ];
    for ( int i = 0; i < results.length; i++ )
    {
      results[ i ] = before + elements[ i ] + after;
    }
    return results;
  }

  protected final String s( final String... commands )
  {
    return join( ';', commands );
  }
}
