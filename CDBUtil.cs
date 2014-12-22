using System;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Data;
using System.Text;
using System.Collections;
using System.Collections.Generic;

namespace DBHelper
{   
  /// <summary>
  /// Summary description for CDBUtil.
  /// </summary>
  public class CDBUtil
  {     
    /// <summary>
    /// Do not allow any instances of this object. it is a static resource.
    /// </summary>
    private CDBUtil()
    { 
    }
    
    /// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(String strSQL)
    { 
      return ExecuteReader(CConfigurationManager.ConnectionString, CommandType.Text, strSQL);
    }
    
    
    /// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(String strSQL, params SqlParameter[] aParameters)
    { 
      return ExecuteReader(CConfigurationManager.ConnectionString, CommandType.Text, strSQL, aParameters);
    }

    /// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(CommandType enuCommandType, String strSQL, params SqlParameter[] aParameters)
    { 
      return ExecuteReader(CConfigurationManager.ConnectionString, enuCommandType, strSQL, aParameters);
    }
    
    
    /// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(String strSQL, List<SqlParameter> listParameters)
    { 
      return ExecuteReader(CConfigurationManager.ConnectionString, CommandType.Text, strSQL, listParameters.ToArray());
    }
    
    
    /// <summary>
    /// Executes a reader. Creates a new connection using CConfigurationManager.ConnectionString, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlDataReader dbReader = CDBUtil.CreateReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlDataReader ExecuteReader(CommandType enuCommandType, String strSQL, List<SqlParameter> listParameters)
    { 
      return ExecuteReader(CConfigurationManager.ConnectionString, enuCommandType, strSQL, listParameters.ToArray());
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
    public static SqlDataReader ExecuteReader(String sConnection, CommandType enuCmdType, string sSql, params SqlParameter[] aParameters)
    { 
      SqlConnection dbConn = null;
      SqlCommand dbCmd = null;
      
      try
      { 
        dbConn = new SqlConnection(sConnection);
        dbCmd = dbConn.CreateCommand();
        dbCmd.CommandType = enuCmdType;
        dbCmd.CommandText = sSql;
        
        //add parameters
        if (aParameters != null)
        { 
          foreach (SqlParameter oParam in aParameters)
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
    
    
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteNonQuery(string sSql)
    { 
      return ExecuteNonQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }
    
    
    
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteNonQuery(string sSql, params SqlParameter[] aParameters)
    { 
      return ExecuteNonQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, aParameters);
    }
    
    
    
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteNonQuery(string sSql, List<SqlParameter> listParameters)
    { 
      return ExecuteNonQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, listParameters.ToArray()); 
    }
    
    
    /// <summary>
    /// Executes a parameterized query. Returns number of rows affected.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static int ExecuteNonQuery(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    { 
      return ExecuteNonQuery(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);
    }
    
    
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
    public static int ExecuteNonQuery(string sConnection, CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    { 
      //using statement will ensure we close the connection no matter what happens
      SqlCommand dbCmd = null;
      try
      { 
        using (SqlConnection dbConn = new SqlConnection(sConnection))
        { 
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;
          dbCmd.CommandTimeout = 30;
          
          //add parameters
          if (aParameters != null)
          {
            foreach (SqlParameter oParam in aParameters)
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
    
    
    
    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static string ExecuteScalarString(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalarString(CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static string ExecuteScalarString(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalarString(CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static string ExecuteScalarString(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      object result = ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);

      if (result == null)
        throw new Exception("ExecuteScalar returned a null object. In ref to SQL: \n<BR/>" + sSql + "\n<BR/>");
      else
        return result.ToString();

    }



    /// <summary>
    /// Executes a scalar. Returns result as int or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteScalarInt32(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalarInt32(CommandType.Text, sSql, aParameters);
    }

    /// <summary>
    /// Executes a scalar. Returns result as int or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteScalarInt32(string sSql)
    { 
      return ExecuteScalarInt32(CommandType.Text, sSql, null);
    }


    /// <summary>
    /// Executes a scalar. Returns result as objectDouble
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static int ExecuteScalarInt32(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalarInt32(CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as objectDouble
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static int ExecuteScalarInt32(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      object result = ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);

      if (result == null)
        throw new Exception("ExecuteScalar returned a null object. In ref to SQL: \n<BR/>" + sSql + "\n<BR/>");
      else
        return Convert.ToInt32(result);

    }



    /// <summary>
    /// Executes a scalar. Returns result as int or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static Int64 ExecuteScalarInt64(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalarInt64(CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as object or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static Int64 ExecuteScalarInt64(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalarInt64(CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as object or throws an exception if null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static Int64 ExecuteScalarInt64(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      object result = ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);

      if (result == null)
        throw new Exception("ExecuteScalar returned a null object. In ref to SQL: \n<BR/>" + sSql + "\n<BR/>");
      else
        return Convert.ToInt64(result);

    }



    /// <summary>
    /// Executes a scalar. Returns result as int or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static float ExecuteScalarSingle(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalarSingle(CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as object or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static float ExecuteScalarSingle(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalarSingle(CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as object or throws an exception if null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static float ExecuteScalarSingle(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      object result = ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);

      if (result == null)
        throw new Exception("ExecuteScalar returned a null object. In ref to SQL: \n<BR/>" + sSql + "\n<BR/>");
      else
        return Convert.ToSingle(result);

    }



    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static double ExecuteScalarDouble(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalarDouble(CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static double ExecuteScalarDouble(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalarDouble(CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as double or throws an exception if null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static double ExecuteScalarDouble(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      object result = ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);

      if (result == null)
        throw new Exception("ExecuteScalar returned a null object. In ref to SQL: \n<BR/>" + sSql + "\n<BR/>");
      else
        return Convert.ToDouble(result);

    }





    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql)
    { 
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }

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
    
    
    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static object ExecuteScalar(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static object ExecuteScalar(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteScalar(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);
    }



    /// <summary>
    /// Executes a scalar. Returns result as object or null.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static object ExecuteScalar(string sConnection, CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      //using statement will ensure we close the connection no matter what happens
      SqlCommand dbCmd = null;
      
      try
      { 
        using (SqlConnection dbConn = new SqlConnection(sConnection))
        {
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;

          //add parameters
          if (aParameters != null)
          { 
            foreach (SqlParameter oParam in aParameters)
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




    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, null);
    }

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
    
    
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static DataSet ExecuteQuery(string sConn, string sSql, params SqlParameter[] aParameters)
    { 
      return ExecuteQuery(sConn, CommandType.Text, sSql, aParameters);
    }

    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, aParameters);
    }



    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    public static DataSet ExecuteQuery(string sSql, List<SqlParameter> listParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, CommandType.Text, sSql, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    /// <param name="enuCommandType"></param>
    /// <param name="sSql"></param>
    /// <param name="aParameters"></param>
    /// <returns></returns>
    public static DataSet ExecuteQuery(CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      return ExecuteQuery(CConfigurationManager.ConnectionString, enuCommandType, sSql, aParameters);
    }

    
    
    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    public static DataSet ExecuteQuery(string sConnection, CommandType enuCommandType, string sSql, params SqlParameter[] aParameters)
    {
      //using statement will ensure we close the connection no matter what happens
      SqlCommand dbCmd = null;
      
      try
      {
        using (SqlConnection dbConn = new SqlConnection(sConnection))
        { 
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;
          
          //add parameters
          if (aParameters != null)
          { 
            foreach (SqlParameter oParam in aParameters)
              dbCmd.Parameters.Add(oParam);
          }
          
          SqlDataAdapter custDA = new SqlDataAdapter(dbCmd);
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


    #region -- SQL Compact --

    /// <summary>
    /// Executes a reader. Creates a new connection, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlCeDataReader dbReader = CDBUtil.ExecuteCompactReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlCeDataReader ExecuteCompactReader(String sConnection, String strSQL)
    {
      return ExecuteCompactReader(sConnection, CommandType.Text, strSQL);
    }

    /// <summary>
    /// Executes a reader. Creates a new connection, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlCeDataReader dbReader = CDBUtil.ExecuteCompactReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlCeDataReader ExecuteCompactReader(String sConnection, String strSQL, params SqlCeParameter[] aParameters)
    {
      return ExecuteCompactReader(sConnection, CommandType.Text, strSQL, aParameters);
    }

    
    /// <summary>
    /// Executes a reader. Creates a new connection, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlCeDataReader dbReader = CDBUtil.ExecuteCompactReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>    
    public static SqlCeDataReader ExecuteCompactReader(String sConnection, String strSQL, List<SqlCeParameter> listParameters)
    {
      return ExecuteCompactReader(sConnection, CommandType.Text, strSQL, listParameters.ToArray());
    }


    /// <summary>
    /// Executes a reader. Creates a new connection, opens connection and executes 
    /// reader with CommandBehavior.CloseConnection option. You must close the READER when done, that will close the connection. 
    /// USAGE EXAMPLE: using (SqlCeDataReader dbReader = CDBUtil.ExecuteCompactReader("select * from module order by GroupNum, GroupItemOrder;")){}
    /// </summary>
    /// <param name="strSQL"></param>
    /// <returns></returns>
    public static SqlCeDataReader ExecuteCompactReader(String sConnection, CommandType enuCmdType, string sSql, params SqlCeParameter[] aParameters)
    {
      SqlCeConnection dbConn = null;
      SqlCeCommand dbCmd = null;

      try
      {
        dbConn = new SqlCeConnection(sConnection);
        dbCmd = dbConn.CreateCommand();
        dbCmd.CommandType = CommandType.Text;
        dbCmd.CommandText = sSql;

        //add parameters
        if (aParameters != null)
        {
          foreach (SqlCeParameter oParam in aParameters)
            dbCmd.Parameters.Add(oParam);
        }

        dbConn.Open();
        return dbCmd.ExecuteReader(CommandBehavior.CloseConnection);

      }
      catch (Exception exp)
      {
        try
        {
          //if we fail to execute reader, we MUST close the connection, even if caller used the using construct on the reader.
          //If exception occurs while dbCmd.ExecuteReader() is called, the connection would remain open until GC runs.
          if (dbConn != null)
            dbConn.Close();
        }
        catch (Exception) { }

        //ADD SQL code and parameters to exception information, and throw.
        throw new Exception("Exception in CDBUtil.ExecuteCompactReader(). Connection Str: " + sConnection + ";  SQL: " +
          sSql + (dbCmd != null ? "; Parameters: " + GetCompactCommandParams(dbCmd) : ""), exp);
      }
    }


    /// <summary>
    /// Returns command parameters as a string used for debugging and logging SQL exceptions, or Exception description if it occurs.
    /// </summary>
    /// <param name="dbCmd"></param>
    /// <returns></returns>
    private static string GetCompactCommandParams(SqlCeCommand dbCmd)
    {
      try
      {
        StringBuilder sb = new StringBuilder();
        if (dbCmd != null && dbCmd.Parameters != null)
        {
          for (int i = 0; i < dbCmd.Parameters.Count; i++)
            if (dbCmd.Parameters[ i ] != null && dbCmd.Parameters[ i ].ParameterName != null && dbCmd.Parameters[ i ].Value != null)
              sb.Append(dbCmd.Parameters[ i ].ParameterName + ": " + dbCmd.Parameters[ i ].Value.ToString() + ";  ");
        }

        return sb.ToString();
      }
      catch (Exception exp)
      {
        //return exception description. do NOT throw an exception from this method!!
        return exp.ToString();
      }
    }


    /// <summary>
    /// Executes a query and closes connection. Returns dataset as a result.
    /// </summary>
    public static DataSet ExecuteCompactQuery(string sConnection, CommandType enuCommandType, string sSql, params SqlCeParameter[] aParameters)
    {
      //using statement will ensure we close the connection no matter what happens
      SqlCeCommand dbCmd = null;

      try
      {
        using (SqlCeConnection dbConn = new SqlCeConnection(sConnection))
        {
          //execute the sql command
          dbCmd = dbConn.CreateCommand();
          dbCmd.CommandType = enuCommandType;
          dbCmd.CommandText = sSql;

          //add parameters
          if (aParameters != null)
          {
            foreach (SqlCeParameter oParam in aParameters)
              dbCmd.Parameters.Add(oParam);
          }

          SqlCeDataAdapter custDA = new SqlCeDataAdapter(dbCmd);
          DataSet custDS = new DataSet();
          custDA.Fill(custDS);
          return custDS;
        }
      }
      catch (Exception exp)
      {
        throw new Exception("Exception in CDBUtil.ExecuteQuery(). Connection Str: " + sConnection + ";  SQL: " +
          sSql + (dbCmd != null ? "; Parameters: " + GetCompactCommandParams(dbCmd) : ""), exp);
      }
    }

    #endregion -- SQL Compact --




    public static bool DbTableExists(string strTableNameAndSchema, string strConnection)
    { 
      return Convert.ToBoolean(CDBUtil.ExecuteScalar(strConnection, CommandType.Text, "IF OBJECT_ID(@TableName, 'U') IS NOT NULL SELECT 'true' ELSE SELECT 'false'", new SqlParameter("@TableName", strTableNameAndSchema)));
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

				using (SqlConnection conn = new SqlConnection(connectionString))
				{
					string sqlString = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' " +
														 "ORDER BY ORDINAL_POSITION ASC";

					SqlCommand selectCMD = new SqlCommand(sqlString, conn);
					selectCMD.CommandTimeout = 20;

					SqlDataAdapter custDA = new SqlDataAdapter();
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
      using (SqlConnection conn = new SqlConnection(strConnection))
      {
        SqlCommand selectCMD = new SqlCommand("SELECT * FROM INFORMATION_SCHEMA.TABLES", conn);
        selectCMD.CommandTimeout = 30;
        SqlDataAdapter custDA = new SqlDataAdapter();
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
    private static string GetCommandParams(SqlCommand dbCmd)
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


    
    
    public static void WriteToLog(string sTitle, string sSource, string sMessage, int iMailMessageType, string sIpAddress)
    { 
      try
      { 
        string sql = "insert into message(msgtimestamp, msgtype, msgsource, msgtitle, msg, sessionid, userid, serverip) " +
              "values(CONVERT(datetime, GetDate()), @MsgType, @MsgSource, @MsgTitle, @Msg, 0, 0, @ServerIP);";
        
        CDBUtil.ExecuteNonQuery(sql,
                                new SqlParameter("@MsgType", iMailMessageType),
                                new SqlParameter("@MsgSource", sSource),
                                new SqlParameter("@MsgTitle", sTitle),
                                new SqlParameter("@Msg", sMessage),
                                new SqlParameter("@ServerIP", sIpAddress));
        
      }
      catch (Exception ex)
      { 
        //TODO: can not leave unhandled exception
      }
      
    }
    
    
    public static void WriteToLogAndSendMail(string sTitle, string sSource, string sMailMessage, int iMailMessageType, string sMailTo, string sIpAddress)
    { 
      WriteToLog(sTitle, sSource, sMailMessage, iMailMessageType, sIpAddress);
      SendSystemEmail(sMailTo, sTitle, sSource, sMailMessage, "", "", "", "", "");
    }
    
    
    public static void SendSystemEmail(string sTo, string sSubject, string sSource, string sBody, string sBodyHtml,
                                      string sCC, string sBCC, string sReplyTo, string sFrom)
    { 
      
      //we can go ahead and create the new email record.
      //------------------------------------------------
      try
      { 
        
        //insert a record in the database.
        string strSQL = "INSERT INTO EmailOutbox(" +
            "FromField" +
            ",ToField" +
            ",CCField" +
            ",BccField" +
            ",ReplyTo" +
            ",Subject" +
            ",Body" +
            ",BodyHtml" +
            ",Created" +
            ",Sent" +
            ",SourceName" +
            ",SourceCode" +
            ",Notes) " +
          "VALUES(" +
            "@FromField" +
           ",@ToField" +
           ",@CCField" +
           ",@BccField" +
           ",@ReplyTo" +
           ",@Subject" +
           ",@Body" +
           ",@BodyHtml" +
           ",@Created" +
           ",@Sent" +
           ",@SourceName" +
           ",@SourceCode" +
           ",@Notes)";
        
        int nAffectedRows = CDBUtil.ExecuteNonQuery(strSQL, 
                                new SqlParameter("@FromField", sFrom),
                                new SqlParameter("@ToField", sTo),
                                new SqlParameter("@CCField", sCC),
                                new SqlParameter("@BccField", sBCC),
                                new SqlParameter("@ReplyTo", sReplyTo),
                                new SqlParameter("@Subject", sSubject),
                                new SqlParameter("@Body", sBody),
                                new SqlParameter("@BodyHtml", sBodyHtml),
                                new SqlParameter("@Created", DateTime.Now),
                                new SqlParameter("@Sent", (int)0),
                                new SqlParameter("@SourceName", sSource),
                                new SqlParameter("@SourceCode", (int)0),
                                new SqlParameter("@Notes", ""));
        
      }
      catch (Exception)
      { 
        //TODO: can not leave unhandled exception
      }
      
    }
    
    
    

    
  }
}
