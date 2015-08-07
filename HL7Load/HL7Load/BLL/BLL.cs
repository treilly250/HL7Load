using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.IO;

namespace HL7Load
{
    class BLL
    {
        private static Logger Logger = null;
        private static DAL DAL = null;
        private static List<int> UserIdList = null;
        private static DateTime ImplausibleDateTime = new DateTime(1900, 1, 1);

        public BLL(Settings settings)
        {
            Logger = settings.Logger;
            DAL = new DAL(settings);
            
            // Keep a list of users for which test results have been added
            UserIdList = new List<int>();
        }

        public List<Observation> GetObservationList()
        {
            List<PatientIdentification> patientIdList = DAL.GetAutoMatchMappings();

            List<Observation> observationList = DAL.GetObservationList();
            foreach(Observation obx in observationList)
            {
                if (obx.UserId == 0)
                {
                    GetAutoMatchMappedUserId(obx, patientIdList);
                }
            }

            return observationList;
        }

        public void ProcessFullMatch(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") WAS found in our user database, as user (" + obx.UserId + "), and the test result was loaded.";
            Logger.LogLine(messageString);
            DAL.InsertNewTestResult(obx);
            DAL.UpdateProcessedObservationStatus(obx, true, messageString);

            // Add user ID to the user ID list, if not already in it
            if (!UserIdList.Contains(obx.UserId))
            {
                UserIdList.Add(obx.UserId);
            }
        }

        public void ProcessNameMismatch(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") was NOT found in our user database.";
            Logger.LogLine(messageString);
            DAL.UpdateProcessedObservationStatus(obx, false, messageString);
        }

        public void ProcessObservationMismatch(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") found as user (" + obx.UserId + "), BUT the test result was NOT loaded, because there was no USHC test corresponding to observation (" + obx.ObservationId + ").";
            Logger.LogLine(messageString);
            DAL.UpdateProcessedObservationStatus(obx, false, messageString);
        }

        public void UpdateUserScores()
        {
            foreach(int userId in UserIdList)
            {
                DAL.UpdateUserScores(userId);
            }
        }

        private void GetAutoMatchMappedUserId(Observation obx, List<PatientIdentification> patientIdList)
        {
            string obxLastName = obx.LastName.ToLower();
            string obxFirstName = obx.FirstName.ToLower();
            string obxGender = obx.Gender.ToLower();
            string obxSSN = obx.SSN;
            DateTime obxDOB = ImplausibleDateTime;  // A perfectly implausible date of birth...
            DateTime.TryParseExact(obx.DOB, DAL.DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out obxDOB);

            foreach (PatientIdentification patientId in patientIdList)
            {
                string patientIdSSN = patientId.SSN;
                string patientIdLastName = patientId.LastName.ToLower();
                string patientIdFirstName = patientId.FirstName.ToLower();
                string patientIdGender = patientId.Gender.ToLower();

                if (obxSSN == patientIdSSN
                    &&
                    obxLastName == patientIdLastName
                    &&
                    obxFirstName == patientIdFirstName
                    &&
                    obxGender == patientIdGender)
                {
                    DateTime patientIdDOB = ImplausibleDateTime;  // A perfectly implausible date of birth...
                    DateTime.TryParseExact(patientId.DOB, DAL.DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out patientIdDOB);
                    if (obxDOB.Date == patientIdDOB.Date)
                    {
                        obx.UserId = patientId.MatchedToUserId;
                    }
                }
                return;
            }
        }
    }
}
