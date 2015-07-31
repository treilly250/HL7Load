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
                DAL dal = new DAL(Settings);

                // Get a list of the observations to be processed, both new ones and previously failed ones
                List<Observation> obxList = dal.GetObservationList();

                foreach (Observation obx in obxList)
                {
                    if (obx.UserId > 0)
                    {
                        string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") WAS found in our user database, as user (" + obx.UserId + "), and the test result was loaded.";
                        Logger.LogLine(messageString);
                        dal.InsertNewTestResult(obx);
                        dal.UpdateProcessedObservationStatus(obx, true, messageString);
                    }
                    else
                    {
                        string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") was not found in our user database.";
                        Logger.LogLine(messageString);
                        dal.UpdateProcessedObservationStatus(obx, false, messageString);
                    }
                }
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
