using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HL7Load
{
    class Settings
    {
        public Logger Logger = null;
        public string SqlConnectionMainString = null;
        public bool FindUsersBySSN = true;
        private string FindUsersBySSNString = null;

        public Settings(Logger logger)
        {
            Logger = logger;
            if (ConfigurationManager.ConnectionStrings["SqlConnectionMain"] != null)
            {
                SqlConnectionMainString = ConfigurationManager.ConnectionStrings["SqlConnectionMain"].ToString();
                logger.LogLine("Main database connection string: " + SqlConnectionMainString);
            }
            if (ConfigurationManager.AppSettings["FindUsersBySSN"] != null)
            {
                FindUsersBySSNString = ConfigurationManager.AppSettings["FindUsersBySSN"].ToString();
                bool.TryParse(FindUsersBySSNString, out FindUsersBySSN);
                logger.LogLine("Finding users based on: " + (FindUsersBySSN ? "SSN" : "UserCN"));
            }
        }
    }
}
