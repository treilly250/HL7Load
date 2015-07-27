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
            DAL dal = new DAL(ConfigurationManager.ConnectionStrings["SqlConnectionHL7"].ToString(),
                              ConfigurationManager.ConnectionStrings["SqlConnectionMain"].ToString());

            // Get a list of the observations to be processed, both new ones and previously failed ones
            List<Observation> obxList = dal.GetObservationList();

            foreach (Observation obx in obxList)
            {
                if (obx.UserId > 0)
                {
                    string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") WAS found in our user database, as user (" + obx.UserId + "), and the test result was loaded.";
                    Console.WriteLine(messageString);
                    dal.InsertNewTestResult(obx);
                    dal.UpdateProcessedObservationStatus(obx, true, messageString);
                }
                else
                {
                    string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") was not found in our user database.";
                    Console.WriteLine(messageString);
                    dal.UpdateProcessedObservationStatus(obx, false, messageString);
                }
            }
        }
    }
}
