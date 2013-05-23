package org.realityforge.dbdiff;

import org.postgresql.Driver;
import org.testng.annotations.Test;

@SuppressWarnings( "UnnecessaryLocalVariable" )
public class PostgresDatabaseDiffTest
  extends AbstractDatabaseDiffTest
{
  private static final String DB1_NAME = "dbdiff_test_db1";
  private static final String DB2_NAME = "dbdiff_test_db2";

  @Test
  public void emptySchema()
    throws Exception
  {
    final String schema = "x";
    final String ddl1 = schema( schema );
    final String ddl2 = ddl1;
    assertMatch( schema, ddl1, ddl2 );
  }

  @Test
  public void emptyMissingSchema()
    throws Exception
  {
    final String schema = "x";
    final String ddl1 = schema( schema );
    final String ddl2 = "";
    assertNotMatch( schema, ddl1, ddl2 );
  }

  @Test
  public void emptySimpleTable()
    throws Exception
  {
    final String schema = "x";
    final String table = "myTable";
    final String ddl1 =
      s( schema( schema ), table( schema, table, column( "ID", "integer" ), column( "TS", "timestamp NOT NULL" ) ) );
    final String ddl2 = ddl1;
    assertMatch( schema, ddl1, ddl2 );
  }

  @Test
  public void emptySimpleTableWithAddedColumn()
    throws Exception
  {
    final String schema = "x";
    final String table = "myTable";
    final String ddl1 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ),
                column( "TS", "timestamp NOT NULL" ),
                pkInlineConstraint( "PK_" + table, "ID" ) ) );
    final String ddl2 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ),
                column( "NewOne", "integer" ),
                column( "TS", "timestamp NOT NULL" ),
                pkInlineConstraint( "PK_" + table, "ID" ) ) );
    assertNotMatch( schema, ddl1, ddl2 );
  }

  @Test
  public void emptySimpleTableWithAddedPkConstraint()
    throws Exception
  {
    final String schema = "x";
    final String table = "myTable";
    final String ddl1 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ) ) );
    final String ddl2 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ),
                pkInlineConstraint( "PK_" + table, "ID" ) ) );
    assertNotMatch( schema, ddl1, ddl2 );
  }

  @Test
  public void emptySimpleTableWithChangedColumnNullType()
    throws Exception
  {
    final String schema = "x";
    final String table = "myTable";
    final String ddl1 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer NOT NULL" ) ) );
    final String ddl2 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ) ) );
    assertNotMatch( schema, ddl1, ddl2 );
  }


  @Test
  public void emptySimpleTableWithChangedColumnScale()
    throws Exception
  {
    final String schema = "x";
    final String table = "myTable";
    final String ddl1 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer[3]" ) ) );
    final String ddl2 =
      s( schema( schema ),
         table( schema,
                table,
                column( "ID", "integer" ) ) );
    assertNotMatch( schema, ddl1, ddl2 );
  }

  protected final String schema( final String schema )
  {
    return "CREATE SCHEMA \"" + schema + "\"";
  }

  protected final String table( final String schema,
                                final String table,
                                final String... elements )
  {
    return "CREATE TABLE \"" + schema + "\".\"" + table + "\"(" + join( ',', elements ) + ")";
  }

  protected final String column( final String name, final String type )
  {
    return "\"" + name + "\" " + type;
  }

  protected final String pkInlineConstraint( final String name, final String... columns )
  {
    return "CONSTRAINT \"" + name + "\" PRIMARY KEY (" + join( ',', quote( columns ) ) + ")";
  }

  protected final String[] quote( final String... commands )
  {
    return quote( "\"", "\"", commands );
  }

  protected final Dialect getDialect()
  {
    return Dialect.postgresql;
  }

  protected final Driver getDriver()
  {
    return new Driver();
  }

  protected final String getDatabase1()
  {
    return getBaseDbURL() + DB1_NAME;
  }

  protected final String getDatabase2()
  {
    return getBaseDbURL() + DB2_NAME;
  }

  protected final String getControlDatabase()
  {
    return getBaseDbURL() + "postgres";
  }

  private String getBaseDbURL()
  {
    final String host = System.getProperty( "test.psql.host", "127.0.0.1" );
    final String port = System.getProperty( "test.psql.port", "5432" );
    return "jdbc:postgresql://" + host + ":" + port + "/";
  }

  protected final void setupDatabases()
    throws Exception
  {
    tearDownDatabases();
    executeSQL( "CREATE DATABASE " + DB1_NAME, getControlDatabase() );
    executeSQL( "CREATE DATABASE " + DB2_NAME, getControlDatabase() );
  }

  protected final void tearDownDatabases()
    throws Exception
  {
    executeSQL( "DROP DATABASE IF EXISTS " + DB1_NAME, getControlDatabase() );
    executeSQL( "DROP DATABASE IF EXISTS " + DB2_NAME, getControlDatabase() );
  }
}
