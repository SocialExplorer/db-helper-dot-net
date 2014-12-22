using System;
using System.Collections.Generic;
using System.Text;

namespace DBHelper
{ 
  
  /// <summary>
  /// This class is reading params from app.config.
  /// In case this is a dll, a config file from the parent application will be used.
  /// </summary>
  internal class CConfigurationManager
  { 
    
    public static string ConnectionString 
    { 
      get 
      { 
        return System.Configuration.ConfigurationManager.ConnectionStrings[ "sewebsiteConnectionString" ].ConnectionString; 
      }
    }
    
    public static string FORCE_MODULE_HOST_URL 
    { 
      get 
      { 
        return System.Configuration.ConfigurationManager.AppSettings[ "ForceModuleHostUrl" ]; 
      }
    }
    
    public static string SMS_SSO_SE_MODULE_ABBREV 
    { 
      get 
      { 
        return System.Configuration.ConfigurationManager.AppSettings[ "PearsonSeModuleAbbrev" ]; 
      }
    }
    
    public static string ADMIN_EMAIL 
    { 
      get 
      { 
        return System.Configuration.ConfigurationManager.AppSettings[ "admin-notify-email" ]; 
      }
    }
  }
}
