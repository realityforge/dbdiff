package org.realityforge.dbdiff;

import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DatabaseDumper
{
  // This lists the allowable postgres table types and should be updated for sql server
  private static final Collection<String> ALLOWABLE_TABLE_TYPES =
    Arrays.asList( "INDEX", "SEQUENCE", "TABLE", "VIEW", "TYPE" );
  private static final String TABLE_TYPE = "table_type";
  private static final String TABLE_NAME = "table_name";
  private static final List<String> ALLOWABLE_TABLE_ATTRIBUTES = Arrays.asList( TABLE_TYPE, TABLE_NAME );
  private static final String COLUMN_NAME = "column_name";
  private static final List<String> ALLOWABLE_COLUMN_ATTRIBUTES =
    Arrays.asList( COLUMN_NAME, "COLUMN_DEF", "ORDINAL_POSITION", "SOURCE_DATA_TYPE", "SQL_DATA_TYPE",
                   "NUM_PREC_RADIX", "COLUMN_SIZE", "TYPE_NAME", "IS_AUTOINCREMENT", "DECIMAL_DIGITS", "DATA_TYPE",
                   "BUFFER_LENGTH", "CHAR_OCTET_LENGTH", "IS_NULLABLE", "NULLABLE", "SQL_DATETIME_SUB", "REMARKS",
                   "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE" );
  private static final String FK_NAME = "fk_name";
  private static final List<String> ALLOWABLE_FOREIGN_KEY_ATTRIBUTES =
    Arrays.asList( FK_NAME, "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                   "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                   "PK_NAME", "DEFERRABILITY" );
  private static final String PK_NAME = "pk_name";
  private static final List<String> ALLOWABLE_PRIMARY_KEY_ATTRIBUTES =
    Arrays.asList( PK_NAME, "COLUMN_NAME", "KEY_SEQ" );
  private static final List<String> ALLOWABLE_TABLE_PRIV_ATTRIBUTES =
    Arrays.asList( "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" );
  private static final List<String> ALLOWABLE_COLUMN_PRIV_ATTRIBUTES =
    Arrays.asList( "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" );

  private final Connection _connection;
  private final List<String> _schemas;

  public DatabaseDumper( final Connection connection,
                         final String[] schemas )
  {
    _connection = connection;
    _schemas = Arrays.asList( schemas );
  }

  public void dump( final Writer w )
    throws Exception
  {
    final DatabaseMetaData metaData = _connection.getMetaData();
    final List<String> schemaSet = getSchema( metaData );
    for ( final String schema : _schemas )
    {
      if ( schemaSet.contains( schema ) )
      {
        emitSchema( w, metaData, schema );
      }
      else
      {
        w.write( "Missing Schema: " + schema + "\n" );
      }
    }
  }

  private void emitSchema( final Writer w, final DatabaseMetaData metaData, final String schema )
    throws Exception
  {
    w.write( "Schema: " + schema + "\n" );
    for ( final LinkedHashMap<String, Object> table : getTablesForSchema( metaData, schema ) )
    {
      final String tableName = (String) table.get( TABLE_NAME );
      final String tableType = (String) table.get( TABLE_TYPE );
      w.write( "\t" + tableType + ": " + tableName + "\n" );

      for ( final LinkedHashMap<String, Object> priv : getTablePrivileges( metaData, schema, tableName ) )
      {
        w.write( "\t\tPRIV    : " + compact( priv ) + "\n" );
      }
      for ( final LinkedHashMap<String, Object> pk : getPrimaryKeys( metaData, schema, tableName ) )
      {
        final String pkName = (String) pk.get( PK_NAME );
        pk.remove( PK_NAME );
        w.write( "\t\tPK      : " + pkName + ": " + compact( pk ) + "\n" );
      }
      for ( final LinkedHashMap<String, Object> column : getColumns( metaData, schema, tableName ) )
      {
        final String columnName = (String) column.get( COLUMN_NAME );
        column.remove( COLUMN_NAME );
        w.write( "\t\tCOLUMN  : " + columnName + ": " + compact( column ) + "\n" );
        final List<LinkedHashMap<String, Object>> privileges =
          getColumnPrivileges( metaData, schema, tableName, columnName );
        for ( final LinkedHashMap<String, Object> priv : privileges )
        {
          w.write( "\t\t\tPRIV    : " + compact( priv ) + "\n" );
        }
      }
      for ( final LinkedHashMap<String, Object> fk : getImportedKeys( metaData, schema, tableName ) )
      {
        final String fkName = (String) fk.get( FK_NAME );
        fk.remove( FK_NAME );
        w.write( "\t\tFK      : " + fkName + ": " + compact( fk ) + "\n" );
      }
    }
    //metaData.getProcedures(  )
    //metaData.getProcedureColumns(  )
    //metaData.getAttributes(  )
    //metaData.getIndexInfo
  }

  private LinkedHashMap compact( final LinkedHashMap<String, Object> column )
  {
    final ArrayList<String> keys = new ArrayList<String>();
    keys.addAll( column.keySet() );
    for ( final String key : keys )
    {
      if ( null == column.get( key ) )
      {
        column.remove( key );
      }
    }
    return column;
  }

  private List<LinkedHashMap<String, Object>> getTablePrivileges( final DatabaseMetaData metaData,
                                                               final String schema,
                                                               final String tablename )
    throws Exception
  {
    final ResultSet columnResultSet = metaData.getTablePrivileges( null, schema, tablename );
    return extractFromRow( columnResultSet, ALLOWABLE_TABLE_PRIV_ATTRIBUTES );
  }

  private List<LinkedHashMap<String, Object>> getColumnPrivileges( final DatabaseMetaData metaData,
                                                               final String schema,
                                                               final String tableName,
                                                               final String columnName )
    throws Exception
  {
    final ResultSet columnResultSet = metaData.getColumnPrivileges( null, schema, tableName, columnName );
    return extractFromRow( columnResultSet, ALLOWABLE_COLUMN_PRIV_ATTRIBUTES );
  }

  private List<LinkedHashMap<String, Object>> getPrimaryKeys( final DatabaseMetaData metaData,
                                                               final String schema,
                                                               final String tablename )
    throws Exception
  {
    final ResultSet columnResultSet = metaData.getPrimaryKeys( null, schema, tablename );
    return extractFromRow( columnResultSet, ALLOWABLE_PRIMARY_KEY_ATTRIBUTES );
  }

  private List<LinkedHashMap<String, Object>> getImportedKeys( final DatabaseMetaData metaData,
                                                               final String schema,
                                                               final String tablename )
    throws Exception
  {
    final ResultSet columnResultSet = metaData.getImportedKeys( null, schema, tablename );
    return extractFromRow( columnResultSet, ALLOWABLE_FOREIGN_KEY_ATTRIBUTES );
  }

  private List<LinkedHashMap<String, Object>> getColumns( final DatabaseMetaData metaData,
                                                          final String schema,
                                                          final String tablename )
    throws Exception
  {
    final ResultSet columnResultSet = metaData.getColumns( null, schema, tablename, null );
    return extractFromRow( columnResultSet, ALLOWABLE_COLUMN_ATTRIBUTES );
  }

  private List<LinkedHashMap<String, Object>> getTablesForSchema( final DatabaseMetaData metaData,
                                                                  final String schema )
    throws Exception
  {
    final List<String> tableTypes = getTableTypes( metaData );
    final ResultSet tablesResultSet =
      metaData.getTables( null, schema, null, tableTypes.toArray( new String[ tableTypes.size() ] ) );
    final List<LinkedHashMap<String, Object>> linkedHashMaps =
      extractFromRow( tablesResultSet, ALLOWABLE_TABLE_ATTRIBUTES );
    Collections.sort( linkedHashMaps, new Comparator<LinkedHashMap<String, Object>>()
    {
      @Override
      public int compare( final LinkedHashMap<String, Object> lhs, final LinkedHashMap<String, Object> rhs )
      {
        final String left = (String) lhs.get( TABLE_TYPE ) + lhs.get( TABLE_NAME );
        final String right = (String) rhs.get( TABLE_TYPE ) + rhs.get( TABLE_NAME );
        return left.compareTo( right );
      }
    } );
    return linkedHashMaps;
  }

  private List<String> getSchema( final DatabaseMetaData metaData )
    throws Exception
  {
    return extractFromRow( metaData.getSchemas(), "table_schem" );
  }

  private List<String> getTableTypes( final DatabaseMetaData metaData )
    throws Exception
  {
    final List<String> supportedTypes =
      extractFromRow( metaData.getTableTypes(), TABLE_TYPE );
    final Iterator<String> iterator = supportedTypes.iterator();
    while ( iterator.hasNext() )
    {
      final String type = iterator.next();
      if ( !ALLOWABLE_TABLE_TYPES.contains( type ) )
      {
        iterator.remove();
      }
    }
    return supportedTypes;
  }

  private <T> List<T> extractFromRow( final ResultSet resultSet, final String key )
    throws Exception
  {
    return map( resultSet, new MapHandler<T>()
    {
      @Override
      public T handle( final Map<String, Object> row )
      {
        return extract( row, key );
      }
    } );
  }

  private List<LinkedHashMap<String, Object>> extractFromRow( final ResultSet resultSet,
                                                              final List<String> keys )
    throws Exception
  {
    return map( resultSet, new MapHandler<LinkedHashMap<String, Object>>()
    {
      @SuppressWarnings( "unchecked" )
      @Override
      public LinkedHashMap<String, Object> handle( final Map<String, Object> row )
      {
        final LinkedHashMap tuple = new LinkedHashMap();
        for ( final String key : keys )
        {
          tuple.put( key.toLowerCase(), extract( row, key ) );
        }
        return tuple;
      }
    } );
  }

  @SuppressWarnings( "unchecked" )
  private <T> T extract( final Map<String, Object> row, final String key )
  {
    if ( row.containsKey( key.toLowerCase() ) )
    {
      return (T) row.get( key.toLowerCase() );
    }
    else if ( row.containsKey( key.toUpperCase() ) )
    {
      return (T) row.get( key.toUpperCase() );
    }
    else
    {
      throw new IllegalStateException( "Unexpected null value for key " + key + " when accessing " + row );
    }
  }

  interface RowHandler
  {
    void handle( Map<String, Object> row );
  }

  private void each( final ResultSet resultSet, final RowHandler handler )
    throws Exception
  {
    for ( final Map<String, Object> row : toList( resultSet ) )
    {
      handler.handle( row );
    }
  }

  interface MapHandler<T>
  {
    T handle( Map<String, Object> row );
  }

  private <T> List<T> map( final ResultSet resultSet, final MapHandler<T> handler )
    throws Exception
  {
    final ArrayList<T> results = new ArrayList<T>();
    each( resultSet, new RowHandler()
    {
      @Override
      public void handle( final Map<String, Object> row )
      {
        results.add( handler.handle( row ) );
      }
    } );
    return results;
  }

  private List<Map<String, Object>> toList( final ResultSet resultSet )
    throws SQLException
  {
    final ResultSetMetaData md = resultSet.getMetaData();
    final int columns = md.getColumnCount();
    final ArrayList<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    while ( resultSet.next() )
    {
      final HashMap<String, Object> row = new HashMap<String, Object>();
      for ( int i = 1; i <= columns; ++i )
      {
        row.put( md.getColumnName( i ), resultSet.getObject( i ) );
      }
      list.add( row );
    }

    return list;
  }
}
