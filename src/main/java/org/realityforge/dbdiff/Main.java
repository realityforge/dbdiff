package org.realityforge.dbdiff;

import java.sql.Connection;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.realityforge.cli.CLArgsParser;
import org.realityforge.cli.CLOption;
import org.realityforge.cli.CLOptionDescriptor;
import org.realityforge.cli.CLUtil;

/**
 * The entry point in which to run the tool.
 */
public class Main
{
  private static final int HELP_OPT = 1;
  private static final int QUIET_OPT = 'q';
  private static final int VERBOSE_OPT = 'v';
  private static final int DATABASE_DRIVER_OPT = 2;
  private static final int DATABASE_DIALECT_OPT = 3;
  private static final int DATABASE_PROPERTY_OPT = 'D';
  private static final int SCHEMA_OPT = 's';

  private static final CLOptionDescriptor[] OPTIONS = new CLOptionDescriptor[]{
    new CLOptionDescriptor( "database-driver",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            DATABASE_DRIVER_OPT,
                            "The jdbc driver to load prior to connecting to the databases." ),
    new CLOptionDescriptor( "database-dialect",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            DATABASE_DIALECT_OPT,
                            "The database dialect to use during diff." ),
    new CLOptionDescriptor( "database-property",
                            CLOptionDescriptor.ARGUMENTS_REQUIRED_2 | CLOptionDescriptor.DUPLICATES_ALLOWED,
                            DATABASE_PROPERTY_OPT,
                            "A jdbc property." ),
    new CLOptionDescriptor( "schema",
                            CLOptionDescriptor.ARGUMENT_REQUIRED | CLOptionDescriptor.DUPLICATES_ALLOWED,
                            SCHEMA_OPT,
                            "A schema to analyze." ),
    new CLOptionDescriptor( "help",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            HELP_OPT,
                            "print this message and exit" ),
    new CLOptionDescriptor( "quiet",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            QUIET_OPT,
                            "Do not output unless an error occurs, just return 0 on no difference.",
                            new int[]{VERBOSE_OPT} ),
    new CLOptionDescriptor( "verbose",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            VERBOSE_OPT,
                            "Verbose output of differences.",
                            new int[]{QUIET_OPT}),
  };

  private static final int NO_DIFFERENCE_EXIT_CODE = 0;
  private static final int DIFFERENCE_EXIT_CODE = 1;
  private static final int ERROR_PARSING_ARGS_EXIT_CODE = 2;
  private static final int ERROR_BAD_DRIVER_EXIT_CODE = 3;
  private static final int ERROR_OTHER_EXIT_CODE = 4;

  private static final int QUIET = 0;
  private static final int NORMAL = 1;
  private static final int VERBOSE = 1;

  private static int c_logLevel = NORMAL;
  private static String c_databaseDriver;
  private static String c_databaseDialect;
  private static String c_database1;
  private static String c_database2;
  private static final Properties c_dbProperties = new Properties();
  private static final ArrayList<String> c_schemas = new ArrayList<String>();

  public static void main( final String[] args )
  {
    if ( !processOptions( args ) )
    {
      System.exit( ERROR_PARSING_ARGS_EXIT_CODE );
      return;
    }

    if ( VERBOSE <= c_logLevel )
    {
      info( "Performing difference between databases" );
    }

    final Driver driver = loadDatabaseDriver();
    if ( null == driver )
    {
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return;
    }

    final boolean difference = diff( driver );

    if ( difference )
    {
      if ( NORMAL <= c_logLevel )
      {
        error( "Difference found between databases" );
      }
      System.exit( DIFFERENCE_EXIT_CODE );
    }
    else
    {
      if ( NORMAL <= c_logLevel )
      {
        info( "No difference found between databases" );
      }
      System.exit( NO_DIFFERENCE_EXIT_CODE );
    }
  }

  private static Driver loadDatabaseDriver()
  {
    try
    {
      return (Driver)Class.forName( c_databaseDriver ).newInstance();
    }
    catch ( final Exception e )
    {
      error( "Unable to load database driver " + c_databaseDriver + " due to " + e );
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return null;
    }
  }

  static class DifferenceReporter
  implements DatabaseDifferenceHandler
  {
    boolean _differenceFound;
    @Override
    public void databaseDifference( final String description, final boolean added )
    {
      if ( NORMAL <= c_logLevel )
      {
        info( description + " " + (added ? "added" : "removed") + " from database" );
      }
      _differenceFound = true;
    }
  }

  private static boolean diff( final Driver driver )
  {
    try
    {
      final Connection connection1 = driver.connect( c_database1, c_dbProperties );
      final Connection connection2 = driver.connect( c_database2, c_dbProperties );

      final DifferenceReporter handler = new DifferenceReporter();
      final DatabaseDiffProcessor processor =
        new DatabaseDiffProcessor( connection1, connection2, c_databaseDialect, handler );
      processor.performDiff();

      connection1.close();
      connection2.close();

      return handler._differenceFound;
    }
    catch ( final Throwable t )
    {
      error( "Error performing diff: " + t );
      System.exit( ERROR_OTHER_EXIT_CODE );
      return false;
    }
  }

  private static boolean processOptions( final String[] args )
  {
    // Parse the arguments
    final CLArgsParser parser = new CLArgsParser( args, OPTIONS );

    //Make sure that there was no errors parsing arguments
    if ( null != parser.getErrorString() )
    {
      error( parser.getErrorString() );
      return false;
    }

    // Get a list of parsed options
    @SuppressWarnings( "unchecked" ) final List<CLOption> options = parser.getArguments();
    for ( final CLOption option : options )
    {
      switch ( option.getId() )
      {
        case CLOption.TEXT_ARGUMENT:
          if ( null == c_database1 )
          {
            c_database1 = option.getArgument();
          }
          else if ( null == c_database2 )
          {
            c_database2 = option.getArgument();
          }
          else
          {
            error( "Unexpected 3rd test argument: " + option.getArgument() );
            return false;
          }
          break;
        case SCHEMA_OPT:
        {
          c_schemas.add( option.getArgument() );
          break;
        }
        case DATABASE_PROPERTY_OPT:
        {
          c_dbProperties.setProperty( option.getArgument(), option.getArgument( 1 ) );
          break;
        }
        case DATABASE_DRIVER_OPT:
        {
          c_databaseDriver = option.getArgument();
          break;
        }
        case DATABASE_DIALECT_OPT:
        {
          c_databaseDialect = option.getArgument();
          if ( !"mssql".equals( c_databaseDialect ) )
          {
            error( "Unsupported database dialect: " + c_databaseDialect );
            return false;
          }
          break;
        }
        case VERBOSE_OPT:
        {
          c_logLevel = VERBOSE;
          break;
        }
        case QUIET_OPT:
        {
          c_logLevel = QUIET;
          break;
        }
        case HELP_OPT:
        {
          printUsage();
          return false;
        }

      }
    }
    if ( null == c_databaseDriver )
    {
      error( "Database driver must be specified" );
      return false;
    }
    if ( null == c_database1 || null == c_database2 )
    {
      error( "Two jdbc urls must supplied for the databases to check differences" );
      return false;
    }
    if ( VERBOSE <= c_logLevel )
    {
      info( "Database 1: " + c_database1 );
      info( "Database 2: " + c_database2 );
      info( "Database Properties: " + c_dbProperties );
      info( "Schemas: " + c_schemas );
    }

    return true;
  }

  /**
   * Print out a usage statement
   */
  private static void printUsage()
  {
    final String lineSeparator = System.getProperty( "line.separator" );

    final StringBuilder msg = new StringBuilder();

    msg.append( "java " );
    msg.append( Main.class.getName() );
    msg.append( " [options] database1JDBCurl database2JDBCurl" );
    msg.append( lineSeparator );
    msg.append( "Options: " );
    msg.append( lineSeparator );

    msg.append( CLUtil.describeOptions( OPTIONS ).toString() );

    info( msg.toString() );
  }

  private static void info( final String message )
  {
    System.out.println( message );
  }

  private static void error( final String message )
  {
    System.out.println( "Error: " + message );
  }
}
