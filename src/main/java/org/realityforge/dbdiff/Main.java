package org.realityforge.dbdiff;

import java.sql.Driver;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
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
  private static final int CONTEXT_SIZE_OPT = 4;

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
    new CLOptionDescriptor( "context-size",
                            CLOptionDescriptor.ARGUMENT_REQUIRED,
                            CONTEXT_SIZE_OPT,
                            "The number of context lines in the diff." ),
    new CLOptionDescriptor( "help",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            HELP_OPT,
                            "print this message and exit" ),
    new CLOptionDescriptor( "quiet",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            QUIET_OPT,
                            "Do not output unless an error occurs, just return 0 on no difference.",
                            new int[]{ VERBOSE_OPT } ),
    new CLOptionDescriptor( "verbose",
                            CLOptionDescriptor.ARGUMENT_DISALLOWED,
                            VERBOSE_OPT,
                            "Verbose output of differences.",
                            new int[]{ QUIET_OPT } ),
  };

  private static final int NO_DIFFERENCE_EXIT_CODE = 0;
  private static final int DIFFERENCE_EXIT_CODE = 1;
  private static final int ERROR_PARSING_ARGS_EXIT_CODE = 2;
  private static final int ERROR_BAD_DRIVER_EXIT_CODE = 3;
  private static final int ERROR_OTHER_EXIT_CODE = 4;

  private static final int QUIET = 0;
  private static final int NORMAL = 1;
  private static final int VERBOSE = 2;

  private static int c_logLevel = NORMAL;
  private static String c_databaseDriver;
  private static final DatabaseDiff c_diffTool = new DatabaseDiff();
  private static final Logger c_logger = Logger.getAnonymousLogger();

  public static void main( final String[] args )
  {
    setupLogger();
    if ( !processOptions( args ) )
    {
      System.exit( ERROR_PARSING_ARGS_EXIT_CODE );
      return;
    }

    if ( c_logger.isLoggable( Level.FINE ) )
    {
      c_logger.log( Level.INFO, "Performing difference between databases" );
    }

    final Driver driver = loadDatabaseDriver();
    if ( null == driver )
    {
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return;
    }

    c_diffTool.setDriver( driver );
    boolean difference;
    try
    {
      difference = c_diffTool.diff();
    }
    catch ( final Throwable t )
    {
      c_logger.log( Level.SEVERE, "Error: " + "Error performing diff: " + t );
      System.exit( ERROR_OTHER_EXIT_CODE );
      return;
    }

    if ( difference )
    {
      if ( c_logger.isLoggable( Level.INFO ) )
      {
        c_logger.log( Level.SEVERE, "Error: " + "Difference found between databases" );
      }
      System.exit( DIFFERENCE_EXIT_CODE );
    }
    else
    {
      if ( c_logger.isLoggable( Level.INFO ) )
      {
        c_logger.log( Level.INFO, "No difference found between databases" );
      }
      System.exit( NO_DIFFERENCE_EXIT_CODE );
    }
  }

  private static void setupLogger()
  {
    c_logger.setUseParentHandlers( false );
    final ConsoleHandler handler = new ConsoleHandler();
    handler.setFormatter( new RawFormatter() );
    c_logger.addHandler( handler );
    c_diffTool.setLogger( c_logger );
  }

  private static Driver loadDatabaseDriver()
  {
    try
    {
      return (Driver) Class.forName( c_databaseDriver ).newInstance();
    }
    catch ( final Exception e )
    {
      c_logger.log( Level.SEVERE, "Error: " + "Unable to load database driver " + c_databaseDriver + " due to " + e );
      System.exit( ERROR_BAD_DRIVER_EXIT_CODE );
      return null;
    }
  }

  private static boolean processOptions( final String[] args )
  {
    // Parse the arguments
    final CLArgsParser parser = new CLArgsParser( args, OPTIONS );

    //Make sure that there was no errors parsing arguments
    if ( null != parser.getErrorString() )
    {
      c_logger.log( Level.SEVERE, "Error: " + parser.getErrorString() );
      return false;
    }

    // Get a list of parsed options
    @SuppressWarnings( "unchecked" ) final List<CLOption> options = parser.getArguments();
    for ( final CLOption option : options )
    {
      switch ( option.getId() )
      {
        case CLOption.TEXT_ARGUMENT:
          if ( null == c_diffTool.getDatabase1() )
          {
            c_diffTool.setDatabase1( option.getArgument() );
          }
          else if ( null == c_diffTool.getDatabase2() )
          {
            c_diffTool.setDatabase2( option.getArgument() );
          }
          else
          {
            c_logger.log( Level.SEVERE, "Error: " + "Unexpected 3rd test argument: " + option.getArgument() );
            return false;
          }
          break;
        case CONTEXT_SIZE_OPT:
        {
          c_diffTool.setContextSize( Integer.parseInt( option.getArgument() ) );
          break;
        }
        case SCHEMA_OPT:
        {
          c_diffTool.getSchemas().add( option.getArgument() );
          break;
        }
        case DATABASE_PROPERTY_OPT:
        {
          c_diffTool.getDbProperties().setProperty( option.getArgument(), option.getArgument( 1 ) );
          break;
        }
        case DATABASE_DRIVER_OPT:
        {
          c_databaseDriver = option.getArgument();
          break;
        }
        case DATABASE_DIALECT_OPT:
        {
          c_diffTool.setDatabaseDialect( option.getArgument() );
          if ( !DatabaseDumper.MSSQL.equals( c_diffTool.getDatabaseDialect() ) &&
            !DatabaseDumper.POSTGRESQL.equals( c_diffTool.getDatabaseDialect() ))
          {
            c_logger.log( Level.SEVERE, "Error: " + "Unsupported database dialect: " + c_diffTool.getDatabaseDialect() +
                           ". Supported dialects = " + DatabaseDumper.MSSQL + "," +
                           DatabaseDumper.POSTGRESQL );
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
      c_logger.log( Level.SEVERE, "Error: " + "Database driver must be specified" );
      return false;
    }
    if ( null == c_diffTool.getDatabaseDialect() )
    {
      c_logger.log( Level.SEVERE, "Error: " + "Database dialect must be specified" );
      return false;
    }
    if ( null == c_diffTool.getDatabase1() || null == c_diffTool.getDatabase2() )
    {
      c_logger.log( Level.SEVERE, "Error: " + "Two jdbc urls must supplied for the databases to check differences" );
      return false;
    }
    if ( c_logger.isLoggable( Level.FINE ) )
    {
      c_logger.log( Level.INFO, "Database 1: " + c_diffTool.getDatabase1() );
      c_logger.log( Level.INFO, "Database 2: " + c_diffTool.getDatabase2() );
      c_logger.log( Level.INFO, "Database Dialect: " + c_diffTool.getDatabaseDialect() );
      c_logger.log( Level.INFO, "Database Properties: " + c_diffTool.getDbProperties() );
      c_logger.log( Level.INFO, "Schemas: " + c_diffTool.getSchemas() );
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

    c_logger.log( Level.INFO, msg.toString() );
  }
}
