package org.realityforge.dbdiff;

import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DatabaseDumper
{
  private final Connection _connection;
  private final String _dialect;
  private final List<String> _schemas;

  public DatabaseDumper( final Connection connection,
                         final String dialect,
                         final String[] schemas )
  {
    _connection = connection;
    _dialect = dialect;
    _schemas = Arrays.asList( schemas );
  }

  public void dump( final Writer w )
    throws Exception
  {
    final DatabaseMetaData metaData = _connection.getMetaData();
    final List<String> schema = getSchema( metaData );
    for ( final String s : _schemas )
    {
      if( schema.contains( s ) )
      {
        w.write( "Schema: " + s + "\n" );
      }
      else
      {
        w.write( "Missing Schema: " + s + "\n" );
      }
    }
  }

  private List<String> getSchema( final DatabaseMetaData metaData )
    throws Exception
  {
    return map( metaData.getSchemas(), new MapHandler<String>()
    {
      @Override
      public String handle( final Map<String, Object> row )
      {
        return (String) row.get( "table_schem" );
      }
    } );
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
    final ArrayList<T> results = new ArrayList<T>(  );
    for ( final Map<String, Object> row : toList( resultSet ) )
    {
      results.add( handler.handle( row ) );
    }
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
