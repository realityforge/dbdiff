package org.realityforge.dbdiff;

import difflib.DiffUtils;
import difflib.Patch;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseDiff
{
  private Logger _logger;
  private Driver _driver;
  private String _database1;
  private String _database2;
  private final Properties _dbProperties = new Properties();
  private Dialect _dialect;
  private final ArrayList<String> _schemas = new ArrayList<String>();
  private int _contextSize = 10;

  public ArrayList<String> getSchemas()
  {
    return _schemas;
  }

  public Dialect getDialect()
  {
    return _dialect;
  }

  public void setDialect( final Dialect dialect )
  {
    _dialect = dialect;
  }

  public int getContextSize()
  {
    return _contextSize;
  }

  public void setContextSize( final int contextSize )
  {
    _contextSize = contextSize;
  }

  public Logger getLogger()
  {
    return _logger;
  }

  public void setLogger( final Logger logger )
  {
    _logger = logger;
  }

  public Driver getDriver()
  {
    return _driver;
  }

  public void setDriver( final Driver driver )
  {
    _driver = driver;
  }

  public String getDatabase1()
  {
    return _database1;
  }

  public void setDatabase1( final String database1 )
  {
    _database1 = database1;
  }

  public String getDatabase2()
  {
    return _database2;
  }

  public void setDatabase2( final String database2 )
  {
    _database2 = database2;
  }

  public Properties getDbProperties()
  {
    return _dbProperties;
  }

  public boolean diff()
    throws Exception
  {
    final Connection connection1 = _driver.connect( _database1, _dbProperties );
    final Connection connection2 = _driver.connect( _database2, _dbProperties );

    final List<String> diff = performDiff( connection1, connection2 );
    if ( _logger.isLoggable( Level.INFO ) )
    {
      for ( final String s : diff )
      {
        _logger.log( Level.INFO, s );
      }
    }

    connection1.close();
    connection2.close();

    return !diff.isEmpty();
  }

  private List<String> performDiff( final Connection connection1,
                                    final Connection connection2 )
    throws Exception
  {
    final List<String> database1 =
      Arrays.asList( databaseSchemaToString( connection1 ).split( "\n" ) );
    final List<String> database2 =
      Arrays.asList( databaseSchemaToString( connection2 ).split( "\n" ) );

    // Compute diff. Get the Patch object. Patch is the container for computed deltas.
    final Patch patch = DiffUtils.diff( database1, database2 );
    return DiffUtils.generateUnifiedDiff( _database1, _database2, database1, patch, _contextSize );
  }

  private String databaseSchemaToString( final Connection connection )
    throws Exception
  {
    final DatabaseDumper dumper =
      new DatabaseDumper( connection,
                          _dialect,
                          _schemas.toArray( new String[ _schemas.size() ] ) );
    final StringWriter sw = new StringWriter();
    dumper.dump( sw );
    final String databaseDump = sw.toString();
    if ( _logger.isLoggable( Level.FINE ) )
    {
      _logger.log( Level.FINE, "---------------------------------------------------------------" );
      _logger.log( Level.FINE, "Database Dump: " + _database1 );
      _logger.log( Level.FINE, "---------------------------------------------------------------" );
      _logger.log( Level.FINE, databaseDump );
      _logger.log( Level.FINE, "---------------------------------------------------------------" );
    }

    return databaseDump;
  }
}
