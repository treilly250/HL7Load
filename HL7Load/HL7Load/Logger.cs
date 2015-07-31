using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace HL7Load
{
    class Logger
    {
        private StreamWriter LogFile = null;

        public Logger()
        {
            string TempPath = Path.GetTempPath();
            if (TempPath.Last() != '\\')
            {
                TempPath += "\\";
            }
            LogFile = new StreamWriter(TempPath + "log.txt", true);
            LogFile.AutoFlush = true;
            LogLine("HL7 Load - execution begun: " +
                    DateTime.Now.ToLongDateString() +
                    " " +
                    DateTime.Now.ToLongTimeString()
                    );
        }

        public void LogLine(string line)
        {
            LogFile.WriteLine(line);
        }

        public void Close()
        {
            LogLine("HL7 Load - execution completed: " +
                    DateTime.Now.ToLongDateString() +
                    " " +
                    DateTime.Now.ToLongTimeString()
                    );
            LogFile.Close();
        }
    }
}
