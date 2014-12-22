using System;
using System.Reflection;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Data;
using System.Data.SqlServerCe;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace DBHelper
{
  /// <summary>
  /// Class used for copying tables from SQL Server to SQL Compact database.
  /// It is required that System.Data.SqlServerCe.dll is copied into local directory and other dependance dll-s (all related to SQL Compact). 
  /// If this class is used by an ASP.NET make sure that relevant Compact SQL DLLs are used (32 bit or 64 bit, depends on IIS settings).
  /// </summary>
  public class CConvertToSqlCompact : IDisposable
  { 
    
    private Server mSourceServer = null;
    public Server SourceServer { get { return this.mSourceServer; } }
    public delegate void dlgMessageHandler(string message);
    public event dlgMessageHandler Message = null;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="sSqlServer">SQL Server name (source)</param>
    /// <param name="sLogin">Login</param>
    /// <param name="sPassword">Pass</param>
    public CConvertToSqlCompact(string sSqlServer, string sLogin, string sPassword)
    {
      this.mSqlServer       = sSqlServer;
      this.mLogin           = sLogin;
      this.mPassword        = sPassword;      
    }

    /// <summary>
    /// Use this constructor if you are going to use connection string for connecting to SQL Server, 
    /// otherwise, use constructor with parameters.
    /// </summary>
    public CConvertToSqlCompact()
    { 
    }
    
    private string mSqlServer;
    public string SqlServer
    { 
      get { return mSqlServer; }
      set { mSqlServer = value; }
    }
    
    private string mLogin;
    
    public string Login
    {
      get { return mLogin; }
      set { mLogin = value; }
    }
    
    private string mPassword;

    public string Password
    {
      get { return mPassword; }
      set { mPassword = value; }
    }

    private string mCompactFile;
    public string SqlCompactFile
    {
      get { return mCompactFile; }
      set { mCompactFile = value; }
    }

    private string mCompactLogin;
    public string CompactLogin
    {
      get { return mCompactLogin; }
      set { mCompactLogin = value; }
    }

    private string mCompactPassword;
    public string CompactPassword
    {
      get { return mCompactPassword; }
      set { mCompactPassword = value; }
    }

    /// <summary>
    /// Connect to SQL Server
    /// </summary>
    /// <returns></returns>
    public bool ConnectToSqlServer()
    {
      try
      {
        ServerConnection svrConn = new ServerConnection(this.mSqlServer);        
        svrConn.LoginSecure = false;        
        svrConn.Login = this.mLogin;
        svrConn.Password = this.mPassword;
        this.mSourceServer = new Server(svrConn);              
        return true;
      }
      catch (Exception ex)
      { 
        FireMessage("ERROR: Unable to connect to SQL Server: " + this.mSqlServer);
        return false;
      }
    }
    
    public void Close()
    { 
      if (mSourceServer != null && mSourceServer.ConnectionContext != null)
        mSourceServer.ConnectionContext.Disconnect();
      
    }

    /// <summary>
    /// Connect to SQL Server. 
    /// This method is used in case parameterless constructor is used for creating the instance.
    /// </summary>
    /// <returns></returns>
    public bool ConnectToSqlServer(string connString)
    {
      try
      {        
        ServerConnection svrConn = new ServerConnection();
        svrConn.ConnectionString = connString;
        this.mSourceServer = new Server(svrConn);
        return true;
      }
      catch (Exception ex)
      {
        FireMessage("ERROR: Unable to connect to SQL Server: " + this.mSqlServer);
        return false;
      }
    }


    /// <summary>
    /// Get list of databases in SQL Server
    /// </summary>
    public List<string> GetDatabases
    {
      get 
      { 
        FireMessage("Getting list of databases...");
        List<string> databases = new List<string>();
        if (this.SourceServer != null && this.SourceServer.Databases.Count > 0)
        {          
          foreach (Database db in this.SourceServer.Databases)
          { 
            databases.Add(db.Name);            
          }
        }
        FireMessage("Getting databases finished.");
        return databases;
      }
    }

    public bool Contains(string database)
    {
      if (this.SourceServer == null)
        return false;
      return this.SourceServer.Databases.Contains(database);
    }

    public bool Contains(string database, string table)
    {
      if (this.SourceServer == null)
        return false;
      return this.SourceServer.Databases[database].Tables.Contains(table);
    }

    private string mSchemaName;
    public string SchemaName
    {
      get { return mSchemaName; }      
    }
	
    
    /// <summary>
    /// Get list of tables for the specific database in SQL Server
    /// </summary>
    /// <param name="sDatabase"></param>
    /// <returns></returns>
    public List<string> GetTables(string sDatabase)
    {
      FireMessage("Getting list of tables...");
      List<string> tables = new List<string>();
      if (this.SourceServer.Databases.Contains(sDatabase))
      {
        Database db = this.SourceServer.Databases[sDatabase];
        List<string> schemas = new List<string>();

        try
        {
          foreach (Schema schema in db.Schemas)
          {
            if (schema.Name.Substring(0, 3) != "db_")
              schemas.Add(schema.Name);
          }
        }
        catch (Exception ex)
        {
          FireMessage(ex.Message);          
          return tables;
        }

        this.mSchemaName = schemas[0];
        foreach (Table tbl in db.Tables)
        {
          tables.Add(tbl.Name);
        }
      }
      FireMessage("Getting tables finished.");
      return tables;
    }

    /// <summary>
    /// Fires 'Message' event 
    /// </summary>
    /// <param name="msg"></param>
    private void FireMessage(string msg)
    { 
      try
      { 
        if (Message != null)
          Message(msg);
      }
      catch { }
      
    }

    /// <summary>
    /// Copy tables from SQL Server to SQL Compact database.
    /// </summary>
    /// <param name="sDbName">SQL Server source database name</param>
    /// <param name="tablesToCopy">List of tables to copy</param>
    /// <param name="bEncryptDb">Encrypt - yes/no</param>
    /// <param name="sCompactFile">Full path to SQL Compact file (.sdf) to be created/filled.</param>
    /// <param name="sCompactLogin">Sql compact login name</param>
    /// <param name="sCompactPassword">Sql compact login password</param>
    /// <param name="bRewriteExistingFile">Reqrite sdf file if aalready exists</param>
    /// <returns>True in case of success, else returns false.</returns>
    public bool CopyTables(string sDbName, List<string> tablesToCopy, bool bEncryptDb, string sCompactFile, string sCompactLogin, string sCompactPassword, bool bRewriteExistingFile)
    {
      if (sDbName == null || !(sDbName.Length > 0) || tablesToCopy == null || !(tablesToCopy.Count > 0))
      {
        FireMessage("ERROR: SQL database OR database tables are not specified");
        return false;
      }

      if (sCompactFile == null || !(sCompactFile.Length > 0) || sCompactLogin == null || sCompactPassword == null)
      {
        FireMessage("ERROR: SQL Compact parameters are not specified");
        return false;
      }

      if (!this.Contains(sDbName))
      {
        FireMessage("ERROR: SQL Server database: "+sDbName+" doesn't exist");
        return false;
      }

      List<string> existingTables = this.GetTables(sDbName);
      string [] copyTables = tablesToCopy.ToArray();
      
      foreach (string copyTable in copyTables)
      {
        if (!existingTables.Contains(copyTable))
          tablesToCopy.Remove(copyTable);  
      }

      if (!(tablesToCopy.Count > 0))
      {
        FireMessage("ERROR: Specified tables are not member of the database.");
        return false;
      }

      this.mCompactFile     = sCompactFile;
      this.mCompactLogin    = sCompactLogin;
      this.mCompactPassword = sCompactPassword;        

      Database sourceDb = SourceServer.Databases[sDbName];
      bool doCopy = true;
      
      
      //Create the Output SDF file
      FireMessage("Creating Database...");
      FireMessage("Checking output path...");
      
      if (bRewriteExistingFile)
      { 
        if (System.IO.File.Exists(this.mCompactFile))
        { 
          try
          { 
            FireMessage("Compact file already exists. It will be overwritten.");
            File.Delete(this.mCompactFile);
          }
          catch (IOException ex)
          { 
            FireMessage("ERROR: Unable to continue. Error in deleting file: " + mCompactFile);
            return false;
          }
        }
      }
      
      if (doCopy)
      { 
        bool copiedFailed = false;
        
        string mobileConnStr = "Data Source='{0}';LCID={1};Password={2};Encrypt={3}; SSCE:Max Database Size=4091;";
        mobileConnStr = String.Format(mobileConnStr, mCompactFile, mCompactLogin, mCompactPassword, bEncryptDb.ToString().ToUpper());
        
        /*string assemblyPath = Environment.CurrentDirectory + "\\System.Data.SqlServerCe.dll";
        //Test Assembly version
        Assembly asm = Assembly.LoadFrom(assemblyPath);
        AssemblyName asmName = asm.GetName();
        Version ver = asmName.Version;
        
        Type type = asm.GetType("System.Data.SqlServerCe.SqlCeEngine");
        object[] objArray = new object[1];
        objArray[0] = mobileConnStr;
        object engine = Activator.CreateInstance(type, objArray);
        //SqlCeEngine engine = new SqlCeEngine(mobileConnStr);
        
        //Create the database. 
        MethodInfo mi = type.GetMethod("CreateDatabase");
        */

        
        try
        { 
          if (!System.IO.File.Exists(this.mCompactFile) || (bRewriteExistingFile))
          { 
            /*mi.Invoke(engine, null);
            //engine.CreateDatabase();
            FireMessage("Compact database created.");*/

            SqlCeEngine oSqlCeEngine = new SqlCeEngine(mobileConnStr);
            oSqlCeEngine.CreateDatabase();
          }
        }
        catch (TargetInvocationException ex)
        { 
          string sErrorMessage = "ERROR: You do not have permissions to save the file to " + mCompactFile + ". Please select a different destination path and try again. "+ex.Message;
          FireMessage(sErrorMessage);
          throw new Exception(sErrorMessage, ex);          
        }
        
        FireMessage("Connecting to the SQL Server Compact Edition Database...");
        
        //Type connType = asm.GetType("System.Data.SqlServerCe.SqlCeConnection");

        System.Data.IDbConnection conn = new SqlCeConnection(); //(System.Data.IDbConnection)Activator.CreateInstance(connType);
        conn.ConnectionString = mobileConnStr;
        conn.Open();

        FireMessage("Create all the tables...");
        int tblCount = 0;

        //Type cmdType = asm.GetType("System.Data.SqlServerCe.SqlCeCommand");
        System.Data.IDbCommand cmd = new SqlCeCommand();//(System.Data.IDbCommand)Activator.CreateInstance(cmdType);

        //SqlCeCommand cmd = new SqlCeCommand();
        foreach (string tblName in tablesToCopy)
        {
          if (!this.Contains(sDbName ,tblName))
            continue;

          Table tbl = sourceDb.Tables[tblName, this.mSchemaName];
          if (tbl == null)
          {
            FireMessage("ERROR: Table '" + tblName + "' was not found in the selected schema.");
            copiedFailed = true;
            break;
          }
          if (tbl.IsSystemObject)
            continue;


          FireMessage("Scripting table: " + tbl.Name);
          StringBuilder sb = new StringBuilder();
          sb.Append("CREATE TABLE [").Append(tbl.Name).Append("](");
          int colIdx = 0;
          List<string> pKeys = new List<string>();
          foreach (Column col in tbl.Columns)
          {
            if (colIdx > 0)
              sb.Append(", ");

            sb.Append("[").Append(col.Name).Append("]").Append(" ");
            int max = 0;
            switch (col.DataType.SqlDataType)
            {
              case SqlDataType.VarChar:
                max = col.DataType.MaximumLength;
                col.DataType = new DataType(SqlDataType.NVarChar);
                col.DataType.MaximumLength = max;
                break;

              case SqlDataType.Char:
                max = col.DataType.MaximumLength;
                col.DataType = new DataType(SqlDataType.NChar);
                col.DataType.MaximumLength = max;
                break;

              case SqlDataType.Text:
                col.DataType = new DataType(SqlDataType.NText);
                break;

              case SqlDataType.Decimal:
                int scale = col.DataType.NumericScale;
                int precision = col.DataType.NumericPrecision;
                col.DataType = new DataType(SqlDataType.Numeric);
                col.DataType.NumericPrecision = precision;
                col.DataType.NumericScale = scale;
                break;
            }

            sb.Append(col.DataType.SqlDataType.ToString());

            SqlDataType datatype = col.DataType.SqlDataType;
            if (datatype == SqlDataType.NVarChar || datatype == SqlDataType.NChar)
              sb.Append(" (").Append(col.DataType.MaximumLength.ToString()).Append(") ");
            else if (datatype == SqlDataType.Numeric)
              sb.Append(" (").Append(col.DataType.NumericPrecision).Append(",").Append(col.DataType.NumericScale).Append(")");
            
            if (col.InPrimaryKey)
              pKeys.Add(col.Name);
            
            if (!col.Nullable)
              sb.Append(" NOT NULL");

            if (col.DefaultConstraint != null && !String.IsNullOrEmpty(col.DefaultConstraint.Text))
            {
              string def = col.DefaultConstraint.Text.Replace("((", "(").Replace("))", ")");
              sb.Append(" DEFAULT ").Append(col.DefaultConstraint.Text);
            }

            if (col.Identity)
            {
              sb.Append(" IDENTITY (").Append(col.IdentitySeed.ToString()).Append(",").Append(col.IdentityIncrement.ToString()).Append(")");
            }

            colIdx++;
          }
          sb.Append(")");


          cmd.CommandText = sb.ToString();
          cmd.Connection = conn;
          try
          {
            cmd.ExecuteNonQuery();
          }
          catch (Exception ex)
          {
            FireMessage("ERROR: Create table failed: " + ex.Message);
            //copiedFailed = true;
            //throw;
            continue;
            //break;
          }

          //add the PK constraints
          if (pKeys.Count > 0)
          {
            sb = new StringBuilder();
            sb.Append("ALTER TABLE [").Append(tbl.Name).Append("] ADD CONSTRAINT PK_");
            //create the constraint name
            for (int k = 0; k < pKeys.Count; k++)
            {
              if (k > 0)
                sb.Append("_");

              sb.Append(pKeys[k]);
            }

            sb.Append(" PRIMARY KEY(");
            //add the constraint fields
            for (int k = 0; k < pKeys.Count; k++)
            {
              if (k > 0)
                sb.Append(", ");

              sb.Append(pKeys[k]);
            }
            sb.Append(")");

            cmd.CommandText = sb.ToString();
            try
            {
              cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
              FireMessage("ERROR: Create table (" + tblName + ") failed! Failed creating the Primary Key(s): " + ex.Message);
              //copiedFailed = true;
              //throw;
              continue;
              //break;
            }
          }

          //copy the indexes
          FireMessage("Scripting the indexes for table: " + tbl.Name);
          foreach (Index idx in tbl.Indexes)
          {
            if (idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
              continue;

            sb = new StringBuilder();
            sb.Append("CREATE");
            if (idx.IsUnique)
              sb.Append(" UNIQUE");

            //if (!idx.IsClustered)
            //    sb.Append(" CLUSTERED");
            //else
            //    sb.Append(" NONCLUSTERED");

            sb.Append(" INDEX ").Append(idx.Name).Append(" ON [").Append(tbl.Name).Append("](");
            for (int i = 0; i < idx.IndexedColumns.Count; i++)
            {
              if (i > 0)
                sb.Append(", ");

              sb.Append(idx.IndexedColumns[i].Name);
            }
            sb.Append(")");

            cmd.CommandText = sb.ToString();
            try
            {
              cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
              FireMessage("ERROR: Create table (" + tblName + ") failed! Failed creating the indexes: " + ex.Message);
              copiedFailed = true;
              throw;
              //break;
            }
          }
          tblCount++;
        }

        if (!copiedFailed)
        {
          FireMessage("Now copy the data...");
          //if (optionsCtrl1.CopyData)
          {
            FireMessage("Copying database data...");
            foreach (string tblName in tablesToCopy)
            {
              if (!this.Contains(sDbName, tblName))
                continue;

              Table tbl = sourceDb.Tables[tblName];
              if (tbl.IsSystemObject)
                continue;

              FireMessage("Copying " + tbl.RowCount.ToString() + " rows from " + tbl.Name);
              bool hasIdentity = false;
              string alterSql = "ALTER TABLE [{0}] ALTER COLUMN [{1}] IDENTITY({2},{3})";
              string IDColName = "";
              long increment = 1;
              //If the table has an Identity column then we need to re-set the seed and increment
              //This is a hack since SQL Server Compact Edition does not support SET IDENTITY_INSERT <columnname> ON
              foreach (Column col in tbl.Columns)
              {
                if (col.Identity)
                {
                  hasIdentity = true;
                  IDColName = col.Name;
                  alterSql = String.Format(alterSql, tbl.Name, col.Name, "{0}", "{1}");
                }
              }

              //Select SQL
              string sql = "SELECT * FROM [{0}]";

              //Insert Sql
              string insertSql = "INSERT INTO [{0}] ({1}) VALUES ({2})";
              StringBuilder sbColums = new StringBuilder();
              StringBuilder sbValues = new StringBuilder();
              int idx1 = 0;
              foreach (Column col in tbl.Columns)
              {
                if (col.Name != IDColName)
                {
                  if (idx1 > 0)
                  {
                    sbColums.Append(",");
                    sbValues.Append(",");
                  }

                  sbColums.Append("[").Append(col.Name).Append("]");
                  sbValues.Append("?");
                  idx1++;
                }
              }

              insertSql = String.Format(insertSql, tbl.Name, sbColums.ToString(), sbValues.ToString());

              sql = String.Format(sql, tbl.Name);
              DataSet ds = sourceDb.ExecuteWithResults(sql);
              if (ds.Tables.Count > 0)
              {
                if (ds.Tables[0].Rows.Count > 0)
                {
                  int rowCnt = 0;
                  foreach (DataRow row in ds.Tables[0].Rows)
                  {
                    rowCnt++;

                    if (hasIdentity)
                    {
                      long seed = long.Parse(row[IDColName].ToString());
                      seed--;
                      string alterTableForIDColumn = String.Format(alterSql, seed.ToString(), increment.ToString());
                      cmd.CommandText = alterTableForIDColumn;
                      try
                      {
                        cmd.ExecuteNonQuery();
                      }
                      catch (Exception ex)
                      {
                        FireMessage("ERROR: Failed altering the Table (" + tblName + ") for IDENTITY insert: " + ex.Message);
                        copiedFailed = true;
                        throw;
                        //break;
                      }
                    }

                    sbValues = new StringBuilder();
                    cmd.Parameters.Clear();
                    cmd.CommandText = insertSql;
                    for (int i = 0; i < tbl.Columns.Count; i++)
                    {
                      if (tbl.Columns[i].Name != IDColName)
                      {
                        /*
                        Type type1 = asm.GetType("System.Data.SqlServerCe.SqlCeParameter");                        
                        object[] objArray1 = new object[2];
                        objArray1[0] = tbl.Columns[i].Name;
                        objArray1[1] = row[tbl.Columns[i].Name];
                        object p = Activator.CreateInstance(type1, objArray1);
                        */
                        SqlCeParameter oSqlCeParam = new SqlCeParameter(tbl.Columns[i].Name, row[tbl.Columns[i].Name]);

                        cmd.Parameters.Add(oSqlCeParam);
                      }
                    }
                    cmd.CommandText = String.Format(insertSql, sbValues.ToString());
                    try
                    {
                      cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                      FireMessage("ERROR: Copy table (" + tblName + ") data failed: " + ex.Message);
                      copiedFailed = true;
                      throw;
                      //break;
                    }
                  }
                }
              }
            }
            
            //Now add the FK relationships
            if (!copiedFailed)
            {
              FireMessage("Adding ForeignKeys...");
              string fkSql = "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY([{2}]) REFERENCES [{3}] ([{4}])";
              foreach (string tblName in tablesToCopy)
              {
                if (!this.Contains(sDbName, tblName))
                  continue;

                Table tbl = sourceDb.Tables[tblName];
                if (tbl.IsSystemObject)
                  continue;

                int fkCnt = tbl.ForeignKeys.Count;
                int fxIdx = 0;
                foreach (ForeignKey fk in tbl.ForeignKeys)
                {
                  bool bContainsRefTable = false;
                  foreach (string s in tablesToCopy)
                  { 
                    if (string.Compare(s, fk.ReferencedTable, true, System.Globalization.CultureInfo.InvariantCulture) == 0)
                    {
                      bContainsRefTable = true;
                      break;
                    }
                  }
                  if (!bContainsRefTable)
                    continue;

                  fxIdx++;

                  string createFKSql = String.Format(fkSql, tbl.Name, fk.Name, "{0}", fk.ReferencedTable, sourceDb.Tables[fk.ReferencedTable].Indexes[fk.ReferencedKey].IndexedColumns[0].Name);
                  StringBuilder sbFk = new StringBuilder();
                  foreach (ForeignKeyColumn col in fk.Columns)
                  {
                    if (sbFk.Length > 0)
                      sbFk.Append(",");

                    sbFk.Append(col.Name);
                  }
                  createFKSql = String.Format(createFKSql, sbFk.ToString());
                  cmd.CommandText = createFKSql;
                  try
                  {
                    cmd.ExecuteNonQuery();
                  }
                  catch (Exception ex)
                  {
                    FireMessage("ERROR: Creating ForeignKeys failed: " + ex.Message);
                    copiedFailed = true;
                    throw;
                    //break;
                  }
                }
              }
            }
            
            FireMessage("Closing the connection to the SQL Server Compact Edition Database...");
            conn.Close();
            conn.Dispose();
            
            if (!copiedFailed)
            { 
              FireMessage("Complete!");
            }
            else
            { 
              FireMessage("Copy failed!");
              return false;
            }
          }
        }
        else
        { 
          FireMessage("Copy failed!");
          return false;
        }
      }
      
      return true;
      
    }
    
    
    #region IDisposable Members
    
    public void Dispose()
    { 
      try
      { 
        this.Close();
        
        // This object will be cleaned up by the Dispose method.
        // Therefore, you should call GC.SupressFinalize to
        // take this object off the finalization queue 
        // and prevent finalization code for this object
        // from executing a second time.
        GC.SuppressFinalize(this);
        
      }
      catch
      {}
      
      
      
    }

    #endregion
  }
}