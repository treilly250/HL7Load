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
            // Get all mappings - explicit ones for specific mismatches,
            // as well as those that have been marked for automatic reuse
            List<AutoMatch> autoMatchList = DAL.GetAutoMatchMappings();
            List<ExplicitMatch> explicitMatchList = DAL.GetExplicitMatchMappings();

            // Get all unprocessed and failed observations from the HL7 database tables
            List<Observation> observationList = DAL.GetObservationList();

            // Apply explicit and automatic mappings as needed
            foreach(Observation obx in observationList)
            {
                if (obx.UserId == 0 || obx.TestId == 0)
                {
                    // Attempt resolution via explicit mappings
                    GetExplicitMatchMappedUserIdAndTestId(obx, explicitMatchList);
                }
                if (obx.UserId == 0)
                {
                    // If not mapped explicitly, try our automatic mappings
                    GetAutoMatchMappedUserId(obx, autoMatchList);
                }
            }

            return observationList;
        }

        public void ProcessFullMatchAndGoodValue(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") WAS found in our user database, as user (" + obx.UserId + "), and the test result was loaded.";
            Logger.LogLine(messageString);
            DAL.InsertNewTestResult(obx);
            DAL.UpdateProcessedObservationStatus(obx, true, messageString, null, null);

            // Add user ID to the user ID list, if not already in it
            if (!UserIdList.Contains(obx.UserId))
            {
                UserIdList.Add(obx.UserId);
            }

            // If a replacement value was used, mark the DataLoadValueError entry as resolved
            if (obx.DataLoadValueErrorID > 0 &&
                obx.ReplacementValue != null &&
                obx.ResultData == obx.ReplacementValue)
            {
                DAL.ResolveDataLoadValueError(obx.DataLoadValueErrorID);
            }
        }

        public void ProcessInvalidTestValue(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") found as user (" + obx.UserId + "), BUT the test result was NOT loaded, because it appears to be invalid based on the USHC test normal ranges (" + obx.TestId + ": " + obx.TestNormalRange + ", " + obx.TestNormalRangeFemale + ").";
            Logger.LogLine(messageString);
            int valueErrorId = RecordValueErrorForLaterResolution(obx);
            DAL.UpdateProcessedObservationStatus(obx, false, messageString, null, valueErrorId);
        }

        public void ProcessNameMismatch(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") was NOT found in our user database.";
            Logger.LogLine(messageString);
            int mismatchTestId = RecordMismatchForLaterResolution(obx);
            DAL.UpdateProcessedObservationStatus(obx, false, messageString, mismatchTestId, null);
        }

        public void ProcessObservationMismatch(Observation obx)
        {
            string messageString = "User " + obx.UserCN + " (" + obx.FirstName + " " + obx.LastName + ") found as user (" + obx.UserId + "), BUT the test result was NOT loaded, because there was no USHC test corresponding to observation (" + obx.ObservationId + ").";
            Logger.LogLine(messageString);
            int mismatchTestId = RecordMismatchForLaterResolution(obx);
            DAL.UpdateProcessedObservationStatus(obx, false, messageString, mismatchTestId, null);
        }

        public void UpdateUserScores()
        {
            foreach(int userId in UserIdList)
            {
                DAL.UpdateUserScores(userId);
            }
        }

        public bool TestValueIsValid(Observation obx)
        {
            if (ContainsAtLeastOneDigit(obx.TestNormalRange) ||
                ContainsAtLeastOneDigit(obx.TestNormalRangeFemale))
            {
                decimal decimalResultData = 0;
                if (!decimal.TryParse(obx.ResultData, out decimalResultData))
                {
                    return false;
                }
            }
            return true;
        }

        private bool ContainsAtLeastOneDigit(string inputString)
        {
            foreach(char inputChar in inputString.ToCharArray())
            {
                if (char.IsDigit(inputChar))
                {
                    return true;
                }
            }
            return false;
        }

        private int RecordMismatchForLaterResolution(Observation obx)
        {
            int mismatchId = DAL.GetExistingDataLoadMismatch(obx);
            if (mismatchId == 0)
            {
                mismatchId = DAL.InsertDataLoadMismatch(obx);
            }
            return DAL.InsertDataLoadMismatchTest(obx, mismatchId);
        }

        private int RecordValueErrorForLaterResolution(Observation obx)
        {
            return DAL.InsertDataLoadValueError(obx);
        }

        private void GetAutoMatchMappedUserId(Observation obx, List<AutoMatch> autoMatchList)
        {
            string obxLastName = obx.LastName.ToLower();
            string obxFirstName = obx.FirstName.ToLower();
            string obxGender = obx.Gender.ToLower();
            string obxSSN = obx.SSN;
            DateTime obxDOB = ImplausibleDateTime;  // A perfectly implausible date of birth...
            DateTime.TryParseExact(obx.DOB, DAL.DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out obxDOB);

            foreach (AutoMatch autoMatch in autoMatchList)
            {
                string autoMatchSSN = autoMatch.SSN;
                string autoMatchLastName = autoMatch.LastName.ToLower();
                string autoMatchFirstName = autoMatch.FirstName.ToLower();
                string autoMatchGender = autoMatch.Gender.ToLower();

                if (obxSSN == autoMatchSSN
                    &&
                    obxLastName == autoMatchLastName
                    &&
                    obxFirstName == autoMatchFirstName
                    &&
                    obxGender == autoMatchGender)
                {
                    if (obxDOB.Date == autoMatch.DOB.Date)
                    {
                        obx.UserId = autoMatch.MatchedToUserId;
                        return;
                    }
                }
            }
        }

        private void GetExplicitMatchMappedUserIdAndTestId(Observation obx, List<ExplicitMatch> explicitMatchList)
        {
            foreach (ExplicitMatch explicitMatch in explicitMatchList)
            {
                if (obx.DataLoadMismatchTestID == explicitMatch.DataLoadMismatchTestId)
                {
                    if (obx.UserId == 0)
                    {
                        obx.UserId = explicitMatch.MatchedToUserId;
                    }
                    if (obx.TestId == 0)
                    {
                        obx.TestId = explicitMatch.MatchedToTestId;
                    }
                    return;
                }
            }
        }
    }
}
