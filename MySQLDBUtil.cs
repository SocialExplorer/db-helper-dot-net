using System;
using System.Data;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using MySql.Data.MySqlClient;


namespace DBHelper
{   
  /// <summary>
	/// Summary description for MySQLDBUtil.
  /// </summary>
  public class MySQLDBUtil
  {     
    /// <summary>
    /// Do not allow any instances of this object. it is a static resource.
    /// </summary>
		private MySQLDBUtil()
    { 
    }
    
    
		/// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="sConnectionStr"></param>
    /// <param name="enuCmdType"></param>
    /// <param name="sql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static MySqlDataReader ExecuteReader(String sConnection, CommandType enuCmdType, string sSql, params MySqlParameter[] aParameters)
    { 
      MySqlConnection dbConn = null;
      MySqlCommand dbCmd = null;
      
      try
      { 
        dbConn = new MySqlConnection(sConnection);
        dbCmd = dbConn.CreateCommand();
        dbCmd.CommandType = enuCmdType;
        dbCmd.CommandText = sSql;
        
        //add parameters
        if (aParameters != null)
        { 
          foreach (MySqlParameter oParam in aParameters)
            dbCmd.Parameters.Add(oParam);
        }
        
        dbConn.Open();
        return dbCmd.ExecuteReader(CommandBehavior.CloseConnection);
        
      }
      catch(Exception exp)
      { 
        try
        { 
          //if we fail to execute reader, we MUST close the connection, even if caller used the using construct on the reader.
          //If exception occurs while dbCmd.ExecuteReader() is called, the connection would remain open until GC runs.
          if (dbConn != null) 
            dbConn.Close();
        }
        catch(Exception){}
        
        //ADD SQL code and parameters to exception information, and throw.
        throw new Exception("Exception in CDBUtil.ExecuteReader(). Connection Str: " + sConnection + ";  SQL: " +
          sSql + (dbCmd != null ? "; Parameters: " + GetCommandParams(dbCmd) : ""), exp);
      }
    }
    
    /*
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteNonQuery(string sSql)
    { 
      return ExecuteNonQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }
    */
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static int ExecuteNonQuery(string sConn, string sSql)
    { 
      return ExecuteNonQuery(sConn, CommandType.Text, sSql, null);
    }
    
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static int ExecuteNonQuery(string sConnection, CommandType enuCommandType, string sSql, params MySqlParameter[] aParameters)
    { 
      //using statement will ensure we close the connection no matter what happens
      MySqlCommand dbCmd = null;
      try
      { 
        using (MySqlConnection dbConn = new MySqlConnection(sConnection))
        { 
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;
          dbCmd.CommandTimeout = 30;
          
          //add parameters
          if (aParameters != null)
          {
            foreach (MySqlParameter oParam in aParameters)
              dbCmd.Parameters.Add(oParam);
          }
          
          dbConn.Open();
          int nAffectedRows = dbCmd.ExecuteNonQuery();
          dbCmd.Parameters.Clear();
          dbConn.Close();
          
          return nAffectedRows;
          
        }
      }
      catch (Exception exp)
      {
        throw new Exception("Exception in CDBUtil.ExecuteNonQuery(). Connection Str: " + sConnection + ";  SQL: " +
          sSql + (dbCmd != null ? "; Parameters: " + GetCommandParams(dbCmd) : ""), exp);
      }


    }
    /*
    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql)
    { 
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }
		 */

    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static object ExecuteScalar(string sConn, string sSql)
    { 
      return ExecuteScalar(sConn, CommandType.Text, sSql, null);
    }
    
    /*
    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql, params MySqlParameter[] aParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, aParameters);
    }
		*/

	/*
    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql, List<MySqlParameter> listParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, listParameters.ToArray());
    }
	*/
/*
    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static object ExecuteScalar(CommandType enuCommandType, string sSql, params MySqlParameter[] aParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);
    }

*/

    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static object ExecuteScalar(string sConnection, CommandType enuCommandType, string sSql, params MySqlParameter[] aParameters)
    {
      //using statement will ensure we close the connection no matter what happens
      MySqlCommand dbCmd = null;
      
      try
      { 
        using (MySqlConnection dbConn = new MySqlConnection(sConnection))
        {
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;

          //add parameters
          if (aParameters != null)
          { 
            foreach (MySqlParameter oParam in aParameters)
              dbCmd.Parameters.Add(oParam);
          }
          
          dbConn.Open();
          object oResult = dbCmd.ExecuteScalar();
          dbConn.Close();

          return oResult;

        }
      }
      catch(Exception exp)
      { 
        throw new Exception("Exception in CDBUtil.ExecuteScalar(). Connection Str: " + sConnection + ";  SQL: " + 
          sSql + (dbCmd != null ? "; Parameters: " + GetCommandParams(dbCmd) : ""), exp);
      }
      
    }
/*
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }
*/
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static DataSet ExecuteQuery(string sConn, string sSql)
    {
      return ExecuteQuery(sConn, CommandType.Text, sSql, null);
    }
    
    /*
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql, params MySqlParameter[] aParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, aParameters);
    }*/


/*
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql, List<MySqlParameter> listParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, listParameters.ToArray());
    }*/

/*
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static DataSet ExecuteQuery(CommandType enuCommandType, string sSql, params MySqlParameter[] aParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);
    }
*/
    
    
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    public static DataSet ExecuteQuery(string sConnection, CommandType enuCommandType, string sSql, params MySqlParameter[] aParameters)
    {
      //using statement will ensure we close the connection no matter what happens
      MySqlCommand dbCmd = null;
      
      try
      {
        using (MySqlConnection dbConn = new MySqlConnection(sConnection))
        { 
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;
          
          //add parameters
          if (aParameters != null)
          { 
            foreach (MySqlParameter oParam in aParameters)
              dbCmd.Parameters.Add(oParam);
          }
          
          MySqlDataAdapter custDA = new MySqlDataAdapter(dbCmd);
          DataSet custDS = new DataSet();
          custDA.Fill(custDS);
          return custDS;
          
        }
      }
      catch(Exception exp)
      { 
        throw new Exception("Exception in CDBUtil.ExecuteQuery(). Connection Str: " + sConnection + ";  SQL: " +
          sSql + (dbCmd != null ? "; Parameters: " + GetCommandParams(dbCmd) : ""), exp);
      }
    }


 
    public static bool DbTableExists(string tblName, string schemaName, string strConnection)
    { 
			string sqlString = "SELECT COUNT(*) " +
												 "FROM information_schema.tables " +
												 "WHERE table_schema = '" + schemaName + "' " + 
												 "AND table_name = '" + tblName + "'";

			int count = MySQLDBUtil.ExecuteNonQuery(strConnection, sqlString);
			return count > 0;
    }

		/// <summary>
		/// Gets a list of table column names for table specified by table name
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="tableName"></param>
		/// <returns>list of table columns, null if nothing was found</returns>
		public static List<string> GetTableColumnNames(string connectionString, string tableName)
		{
			List<string> columns = null;

			if (CDBUtil.DbTableExists(tableName, connectionString))
			{

				using (MySqlConnection conn = new MySqlConnection(connectionString))
				{
					string sqlString = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' " +
														 "ORDER BY ORDINAL_POSITION ASC";

					MySqlCommand selectCMD = new MySqlCommand(sqlString, conn);
					selectCMD.CommandTimeout = 20;

					MySqlDataAdapter custDA = new MySqlDataAdapter();
					custDA.SelectCommand = selectCMD;

					conn.Open();

					DataSet custDS = new DataSet();
					custDA.Fill(custDS);

					if (custDS != null && custDS.Tables.Count > 0)
					{
						columns = new List<string>();
						foreach (DataRow dr in custDS.Tables[0].Rows)
						{
							columns.Add(dr["COLUMN_NAME"].ToString());
						}
					}

				}
			}

			return columns;
		}
    
    public static List<string> GetTableNames(string strConnection)
    {
      using (MySqlConnection conn = new MySqlConnection(strConnection))
      {
        MySqlCommand selectCMD = new MySqlCommand("SELECT * FROM INFORMATION_SCHEMA.TABLES", conn);
        selectCMD.CommandTimeout = 30;
        MySqlDataAdapter custDA = new MySqlDataAdapter();
        custDA.SelectCommand = selectCMD;

        conn.Open();

        DataSet custDS = new DataSet();
        custDA.Fill(custDS);

        List<string> tables = new List<string>();
        if (custDS != null && custDS.Tables.Count > 0)
        {
          foreach (DataRow dr in custDS.Tables[0].Rows)
          {
            tables.Add(dr["TABLE_NAME"].ToString());
          }
        }
        return tables;
      }

    }
    
    
    
    //TODO: get rid of this method
    public static string DBEncode(String s)
    { 
      if (s == null) return "";
      return s.Replace("'", "''");
      
    }
    
    //TODO: get rid of this method
    public static string DBEncode(string s, int maxLen)
    { 
      if (s == null) return "";
      
      string encoded = s.Replace("'", "''");
      
      if (encoded.Length > maxLen)
        encoded = encoded.Substring(0, maxLen);
      
      return encoded;
      
    }
    
    
    //TODO: get rid of this method
    public static string DBDecode(String s)
    { 
      if (s == null)
        return "";
      else
        return s;
    }
    
    
    /// <summary>
    /// Returns command parameters as a string used for debugging and logging SQL exceptions, or Exception description if it occurs.
    /// </summary>
    /// <param name="dbCmd"></param>
    /// <returns></returns>
    private static string GetCommandParams(MySqlCommand dbCmd)
    {
      try
      {
        StringBuilder sb = new StringBuilder();
        if (dbCmd != null && dbCmd.Parameters != null)
        {
          for (int i = 0; i < dbCmd.Parameters.Count; i++)
            if (dbCmd.Parameters[i] != null && dbCmd.Parameters[i].ParameterName != null && dbCmd.Parameters[i].Value != null)
              sb.Append(dbCmd.Parameters[i].ParameterName + ": " + dbCmd.Parameters[i].Value.ToString() + ";  ");
        }

        return sb.ToString();
      }
      catch (Exception exp)
      {
        //return exception description. do NOT throw an exception from this method!!
        return exp.ToString();
      }

    }


    public static string Truncate(string sqlParam, int nMaxLen)
    {
      if (sqlParam != null && sqlParam.Length > nMaxLen)
        return sqlParam.Substring(0, nMaxLen);

      return sqlParam;
    }


    
  }
}
