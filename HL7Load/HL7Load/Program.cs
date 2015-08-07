using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.Threading.Tasks;

namespace HL7Load
{
    class Program
    {
        static void Main(string[] args)
        {
            Logger Logger = new Logger();
            try
            {
                Settings Settings = new Settings(Logger);
                BLL BLL = new BLL(Settings);

                // Get a list of the observations to be processed, both new ones and previously failed ones
                List<Observation> obxList = BLL.GetObservationList();

                // Process each observation based on whether everything matched up (user and observation type)
                foreach (Observation obx in obxList)
                {
                    if (obx.UserId > 0)
                    {
                        if (obx.TestId > 0)
                        {
                            BLL.ProcessFullMatch(obx);
                        }
                        else
                        {
                            BLL.ProcessObservationMismatch(obx);
                        }
                    }
                    else
                    {
                        BLL.ProcessNameMismatch(obx);
                    }
                }
                BLL.UpdateUserScores();
            }
            catch (Exception ex)
            {
                Logger.LogLine("Terminated with an exception, as follows:");
                Logger.LogLine(ex.Message);
                if (ex.InnerException != null)
                {
                    Logger.LogLine("Inner exception:");
                    Logger.LogLine(ex.InnerException.Message);
                }
                Logger.Close();
            }
        }
    }
}
