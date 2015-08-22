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
    class DAL
    {
        private static Logger Logger = null;
        private static SqlConnection SqlConnectionMain = null;
        private static bool FindUsersBySSN = true;

        private Dictionary<string, int> UserCNCache = new Dictionary<string, int>();
        private Dictionary<string, int> SSNCache = new Dictionary<string, int>();
        public static string[] DateFormats =
                {
                "yyyyMMdd",
                "yyyyMMddHHmmss",
                "yyyy-MM-dd"
                };

        public DAL(Settings settings)
        {
            Logger = settings.Logger;

            SqlConnectionMain = new SqlConnection(settings.SqlConnectionMainString);
            SqlConnectionMain.Open();

            FindUsersBySSN = settings.FindUsersBySSN;
        }

        public List<Observation> GetObservationList()
        {
            List<Observation> obxList = new List<Observation>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get previously processed observations that failed
            cmd.CommandText = "GetMessageObservationsFailed";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Observation obx = new Observation();
                obx.MessageId = UtilitiesDAL.ToString(reader["MessageID"]);
                obx.SequenceId = UtilitiesDAL.ToInt(reader["SequenceID"]);
                obx.CustomerId = UtilitiesDAL.ToString(reader["CustomerID"]);
                obx.ObservationId = UtilitiesDAL.ToString(reader["OBXID"]);
                obx.ObservationIdText = UtilitiesDAL.ToString(reader["OBXIDTEXT"]);
                obx.ResultData = UtilitiesDAL.ToString(reader["OBXRESULT"]);
                obx.ResultUnits = UtilitiesDAL.ToString(reader["OBXUNITS"]);
                obx.ReferenceRange = UtilitiesDAL.ToString(reader["OBXREFRANGE"]);
                obx.TestId = UtilitiesDAL.ToInt(reader["TestID"]);
                obx.TestNormalRange = UtilitiesDAL.ToString(reader["NormalRange"]);
                obx.TestNormalRangeFemale = UtilitiesDAL.ToString(reader["NormalRangeF"]);
                obx.InternalPatientId = UtilitiesDAL.ToString(reader["PIDINTERNALPATIENTID"]);
                obx.SSN = GetSSN(reader);
                obx.LastName = UtilitiesDAL.ToString(reader["PIDLASTNAME"]);
                obx.FirstName = UtilitiesDAL.ToString(reader["PIDFIRSTNAME"]);
                obx.MiddleName = UtilitiesDAL.ToString(reader["PIDMIDDLENAME"]);
                obx.DOB = UtilitiesDAL.ToString(reader["PIDDOB"]);
                obx.Gender = UtilitiesDAL.ToString(reader["PIDGENDER"]);
                obx.StreetAddress = UtilitiesDAL.ToString(reader["PIDSTREET"]);
                obx.OtherAddress = UtilitiesDAL.ToString(reader["PIDOTHER"]);
                obx.City = UtilitiesDAL.ToString(reader["PIDCITY"]);
                obx.State = UtilitiesDAL.ToString(reader["PIDSTATE"]);
                obx.Zip = UtilitiesDAL.ToString(reader["PIDZIP"]);
                obx.DataLoadMismatchTestID = UtilitiesDAL.ToInt(reader["DataLoadMismatchTestID"]);
                obx.DataLoadValueErrorID = UtilitiesDAL.ToInt(reader["DataLoadValueErrorID"]);
                obx.ReplacementValue = UtilitiesDAL.ToNullableString(reader["ReplacementValue"]);
                SpecialObxSetup(obx, reader);
                obxList.Add(obx);
            }
            reader.Close();

            // Get new never-before-processed observations
            cmd.CommandText = "GetMessageObservationsUnattempted";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Observation obx = new Observation();
                obx.MessageId = UtilitiesDAL.ToString(reader["MessageID"]);
                obx.SequenceId = UtilitiesDAL.ToInt(reader["SequenceID"]);
                obx.CustomerId = UtilitiesDAL.ToString(reader["CustomerID"]);
                obx.ObservationId = UtilitiesDAL.ToString(reader["OBXID"]);
                obx.ObservationIdText = UtilitiesDAL.ToString(reader["OBXIDTEXT"]);
                obx.ResultData = UtilitiesDAL.ToString(reader["OBXRESULT"]);
                obx.ResultUnits = UtilitiesDAL.ToString(reader["OBXUNITS"]);
                obx.ReferenceRange = UtilitiesDAL.ToString(reader["OBXREFRANGE"]);
                obx.TestId = UtilitiesDAL.ToInt(reader["TestID"]);
                obx.TestNormalRange = UtilitiesDAL.ToString(reader["NormalRange"]);
                obx.TestNormalRangeFemale = UtilitiesDAL.ToString(reader["NormalRangeF"]);
                obx.InternalPatientId = UtilitiesDAL.ToString(reader["PIDINTERNALPATIENTID"]);
                obx.SSN = GetSSN(reader);
                obx.LastName = UtilitiesDAL.ToString(reader["PIDLASTNAME"]);
                obx.FirstName = UtilitiesDAL.ToString(reader["PIDFIRSTNAME"]);
                obx.MiddleName = UtilitiesDAL.ToString(reader["PIDMIDDLENAME"]);
                obx.DOB = UtilitiesDAL.ToString(reader["PIDDOB"]);
                obx.Gender = UtilitiesDAL.ToString(reader["PIDGENDER"]);
                obx.StreetAddress = UtilitiesDAL.ToString(reader["PIDSTREET"]);
                obx.OtherAddress = UtilitiesDAL.ToString(reader["PIDOTHER"]);
                obx.City = UtilitiesDAL.ToString(reader["PIDCITY"]);
                obx.State = UtilitiesDAL.ToString(reader["PIDSTATE"]);
                obx.Zip = UtilitiesDAL.ToString(reader["PIDZIP"]);
                obx.DataLoadMismatchTestID = 0;
                obx.DataLoadValueErrorID = 0;
                SpecialObxSetup(obx, reader);
                obxList.Add(obx);
            }
            reader.Close();

            FillInUserIdFields(obxList);

            return obxList;
        }

        public List<AutoMatch> GetAutoMatchMappings()
        {
            List<AutoMatch> autoMatchList = new List<AutoMatch>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get previously defined patient ID mappings
            cmd.CommandText = "GetAutoMatchMappings";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                AutoMatch autoMatch = new AutoMatch();
                autoMatch.LastName = UtilitiesDAL.ToString(reader["LastName"]);
                autoMatch.FirstName = UtilitiesDAL.ToString(reader["FirstName"]);
                autoMatch.Gender = UtilitiesDAL.ToString(reader["Gender"]);
                autoMatch.DOB = UtilitiesDAL.ToDateTime(reader["DateOfBirth"]);
                autoMatch.SSN = UtilitiesDAL.ToString(reader["SSN"]);
                autoMatch.MatchedToUserId = UtilitiesDAL.ToInt(reader["MatchedToUserID"]);
                autoMatchList.Add(autoMatch);
            }
            reader.Close();

            return autoMatchList;
        }

        public List<ExplicitMatch> GetExplicitMatchMappings()
        {
            List<ExplicitMatch> explicitMatchList = new List<ExplicitMatch>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get previously defined patient ID mappings
            cmd.CommandText = "GetExplicitMatchMappings";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                ExplicitMatch explicitMatch = new ExplicitMatch();
                explicitMatch.DataLoadMismatchTestId = UtilitiesDAL.ToInt(reader["DataLoadMismatchTestID"]);
                explicitMatch.MatchedToUserId = UtilitiesDAL.ToInt(reader["MatchedToUserID"]);
                explicitMatch.MatchedToTestId = UtilitiesDAL.ToInt(reader["MatchedToTestID"]);
                explicitMatchList.Add(explicitMatch);
            }
            reader.Close();

            return explicitMatchList;
        }

        public void InsertNewTestResult(Observation obx)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            Logger.LogLine("Inserting new test result for:");
            Logger.LogLine("   UserId:" + obx.UserId);
            Logger.LogLine("   TestId:" + obx.UserId);
            Logger.LogLine("   EntryDate:" + obx.UserId);

            // Add a new test result
            cmd.CommandText = "InsertNewTestResult";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@UserId", obx.UserId));
            cmd.Parameters.Add(new SqlParameter("@EntryDate", obx.EntryDate));  // Should come from the MSH segment...
            cmd.Parameters.Add(new SqlParameter("@Test", obx.ObservationIdText));
            cmd.Parameters.Add(new SqlParameter("@TestResult", ""));    // Do we need to compute this on test entry?
            cmd.Parameters.Add(new SqlParameter("@TestId", obx.TestId));
            cmd.Parameters.Add(new SqlParameter("@TestValue", obx.ResultData));
            cmd.Parameters.Add(new SqlParameter("@NormalRange", obx.ReferenceRange));
            cmd.ExecuteNonQuery();
        }

        public void UpdateProcessedObservationStatus(Observation obx, bool success, string message, int? mismatchForLaterResolution, int? valueErrorId)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Add a new test result
            cmd.CommandText = "UpdateProcessedObservationStatus";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@MessageID", obx.MessageId));
            cmd.Parameters.Add(new SqlParameter("@SequenceID", obx.SequenceId));
            cmd.Parameters.Add(new SqlParameter("@Success", success));
            cmd.Parameters.Add(new SqlParameter("@Message", message));
            cmd.Parameters.Add(new SqlParameter("@DataLoadMismatchTestID", (success || !mismatchForLaterResolution.HasValue) ? (object)DBNull.Value : mismatchForLaterResolution.Value));
            cmd.Parameters.Add(new SqlParameter("@DataLoadValueErrorID", valueErrorId.HasValue ? valueErrorId.Value : (object)DBNull.Value));
            cmd.ExecuteNonQuery();
        }

        public void UpdateUserScores(int userId)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Update this user's scores
            cmd.CommandText = "UpdateUserScores";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@UserId", userId));
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch
            {
                Logger.LogLine("Unexpected error when trying to update the scores for user " + userId);
            }
        }

        private string GetSSN(SqlDataReader reader)
        {
            // Do this because the SSN embedded in an undefined component of
            // the Patient ID (Internal) field contains a tilde that ends the
            // interpretation of the contents of this field #3 component
            string ssn = "";

            string fullField = UtilitiesDAL.ToString(reader["PIDINTERNALPATIENTIDFULLFIELD"]);
            string[] componentArray = fullField.Split('^');
            if (componentArray.Length > 4)  // Fail if fewer than 5 components are found
            {
                string selectedComponent = componentArray[4];  // Grab component 5
                int tildeOffset = selectedComponent.IndexOf('~');
                if (tildeOffset >= 0 && tildeOffset < selectedComponent.Length)
                {
                    ssn = selectedComponent.Substring(tildeOffset + 1);
                }
                else if (componentArray.Length > 5)  // Fail if fewer than 6 components are found
                {
                    selectedComponent = componentArray[5];  // Grab component 6
                    tildeOffset = selectedComponent.IndexOf('~');
                    if (tildeOffset >= 0 && tildeOffset < selectedComponent.Length)
                    {
                        ssn = selectedComponent.Substring(tildeOffset + 1);
                    }
                }
                ssn = ssn.Replace("-", "");
            }

            return ssn;
        }

        private void SpecialObxSetup(Observation obx, SqlDataReader reader)
        {
            // Create the UserCN (username) that is used to identify the user for each observation
            string firstNameFirstLetter = (obx.FirstName.Length > 0) ? obx.FirstName.Substring(0, 1) : "";
            string lastName = obx.LastName;
            string lastFourOfSSN = (obx.SSN.Length == 9) ? obx.SSN.Substring(5, 4) : "XXXX";
            obx.UserCN = firstNameFirstLetter + lastName + lastFourOfSSN;

            // Extract the actual date part of the message date field
            string messageDateStringRaw = UtilitiesDAL.ToString(reader["EntryDate"]);
            string messageDateString = "";
            foreach (char ch in messageDateStringRaw.ToCharArray())
            {
                if (!char.IsDigit(ch))
                {
                    break;
                }
                messageDateString += ch;
            }
            DateTime messageDateTime;
            if (!DateTime.TryParseExact(messageDateString, DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out messageDateTime))
            {
                messageDateTime = DateTime.Now;
            }
            obx.EntryDate = messageDateTime;

            int firstComponentSeparatorPosition = obx.ResultData.IndexOf('^');
            if (firstComponentSeparatorPosition > -1)
            {
                obx.ResultData = obx.ResultData.Substring(0, firstComponentSeparatorPosition);
            }

            // Use the replacement value, if given
            if (obx.DataLoadValueErrorID > 0 &&
                obx.ReplacementValue != null)
            {
                obx.ResultData = obx.ReplacementValue;
            }
        }

        private void FillInUserIdFields(List<Observation> obxList)
        {
            foreach(Observation obx in obxList)
            {
                if (GetUserIdBySSN(obx.CustomerId))
                {
                    GetUserIdBasedOnSSN(obx);
                }
                else
                {
                    GetUserIdBasedOnUserCN(obx);
                }
            }
        }

        private void GetUserIdBasedOnUserCN(Observation obx)
        {
            int censusUserId = 0;
            string censusGender = "";
            DateTime censusDOB = new DateTime();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get the matching UserId for the UserCN in the current observation
            cmd.CommandText = "GetUserIdBasedOnUserCN";

            if (UserCNCache.Keys.Contains(obx.UserCN))
            {
                Logger.LogLine("Found " + obx.UserCN + " in the cache");
                obx.UserId = UserCNCache[obx.UserCN];
            }
            else
            {
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new SqlParameter("@UserCN", obx.UserCN));
                reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    censusUserId = UtilitiesDAL.ToInt(reader["UserID"]);
                    censusGender = UtilitiesDAL.ToString(reader["Gender"]);
                    censusDOB = UtilitiesDAL.ToDateTime(reader["DOB"]);
                }
                obx.UserId = censusUserId;
                UserCNCache[obx.UserCN] = censusUserId;
                Logger.LogLine("Added " + obx.UserCN + " to the cache");
                if (censusUserId > 0
                    &&
                    !MatchOnGenderAndDOB(obx, censusGender, censusDOB))
                {
                    obx.UserId = 0;  // Treat as if user was not found, due to gender/DOB mismatch
                }

                reader.Close();
            }
        }

        private void GetUserIdBasedOnSSN(Observation obx)
        {
            int censusUserId = 0;
            string censusGender = "";
            DateTime censusDOB = new DateTime();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get the matching UserId for the SSN in the current observation
            cmd.CommandText = "GetUserIdBasedOnSSN";

            if (obx.SSN.Length < 9)
            {
                Logger.LogLine("SSN (" + obx.SSN + ") is not long enough");
                obx.UserId = 0;
            }
            else if (SSNCache.Keys.Contains(obx.SSN))
            {
                Logger.LogLine("Found " + obx.SSN + " in the cache");
                obx.UserId = SSNCache[obx.SSN];
            }
            else
            {
                try
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new SqlParameter("@SSN", obx.SSN));
                    reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        censusUserId = UtilitiesDAL.ToInt(reader["UserID"]);
                        censusGender = UtilitiesDAL.ToString(reader["Gender"]);
                        censusDOB = UtilitiesDAL.ToDateTime(reader["DOB"]);
                    }
                    obx.UserId = censusUserId;
                    SSNCache[obx.SSN] = censusUserId;
                    Logger.LogLine("Added " + obx.SSN + " to the cache");
                    if (censusUserId > 0
                        &&
                        !MatchOnGenderAndDOB(obx, censusGender, censusDOB))
                    {
                        obx.UserId = 0;  // Treat as if user was not found, due to gender/DOB mismatch
                    }

                    reader.Close();
                }
                catch (Exception ex)
                {
                    Logger.LogLine("Unexpected error when trying to process " + obx.SSN + " (" + ex.Message + ")");
                }
            }
        }

        private bool GetUserIdBySSN(string customerId)
        {
            // Determine the right way to get the user ID, based on customer ID string
            return true;
        }

        private bool MatchOnGenderAndDOB(Observation obx, string censusGender, DateTime censusDOB)
        {
            DateTime obxDOB;

            if (DateTime.TryParseExact(obx.DOB, DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out obxDOB))
            {
                if (censusDOB.Date == obxDOB
                    &&
                    censusGender.ToLower() == obx.Gender.ToLower())
                {
                    return true;
                }
            }
            return false;
        }

        public int GetExistingDataLoadMismatch(Observation obx)
        {
            int existingDataLoadMismatchId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            cmd.CommandText = "GetExistingDataLoadMismatch";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@LastName", obx.LastName));
            cmd.Parameters.Add(new SqlParameter("@FirstName", obx.FirstName));
            cmd.Parameters.Add(new SqlParameter("@Gender", obx.Gender));
            cmd.Parameters.Add(new SqlParameter("@DateOfBirth", obx.DOB));
            cmd.Parameters.Add(new SqlParameter("@SSN", obx.SSN));
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                existingDataLoadMismatchId = Convert.ToInt32(reader.GetValue(0));
            }

            reader.Close();

            return existingDataLoadMismatchId;
        }

        public int InsertDataLoadMismatch(Observation obx)
        {
            int newDataLoadMismatchId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            cmd.CommandText = "InsertDataLoadMismatch";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@LastName", obx.LastName));
            cmd.Parameters.Add(new SqlParameter("@FirstName", obx.FirstName));
            cmd.Parameters.Add(new SqlParameter("@Gender", obx.Gender));
            cmd.Parameters.Add(new SqlParameter("@DateOfBirth", obx.DOB));
            cmd.Parameters.Add(new SqlParameter("@SSN", obx.SSN));
            cmd.Parameters.Add(new SqlParameter("@MatchedToUserID", (obx.UserId == 0) ? (object)DBNull.Value : obx.UserId));
            cmd.Parameters.Add(new SqlParameter("@CustomerID", (obx.CustomerId == "DM") ? 1 : 2));
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                newDataLoadMismatchId = Convert.ToInt32(reader.GetValue(0));
            }

            reader.Close();

            return newDataLoadMismatchId;
        }

        public int InsertDataLoadMismatchTest(Observation obx, int dataLoadMismatchId)
        {
            int newDataLoadMismatchTestId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            cmd.CommandText = "InsertDataLoadMismatchTest";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@DataLoadMismatchID", dataLoadMismatchId));
            cmd.Parameters.Add(new SqlParameter("@MatchedToTestID", (obx.TestId == 0) ? (object)DBNull.Value : obx.TestId));
            cmd.Parameters.Add(new SqlParameter("@ObservationID", obx.ObservationId));
            cmd.Parameters.Add(new SqlParameter("@ObservationTestMismatch", obx.TestId == 0));
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                newDataLoadMismatchTestId = Convert.ToInt32(reader.GetValue(0));
            }

            reader.Close();

            return newDataLoadMismatchTestId;
        }

        public int InsertDataLoadValueError(Observation obx)
        {
            int newDataLoadValueErrorId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            cmd.CommandText = "InsertDataLoadValueError";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@SuppliedValue", obx.ResultData));
            cmd.Parameters.Add(new SqlParameter("@ReferenceRange", obx.ReferenceRange));
            cmd.Parameters.Add(new SqlParameter("@TestID", obx.TestId));
            cmd.Parameters.Add(new SqlParameter("@TestNormalRange", obx.TestNormalRange));
            cmd.Parameters.Add(new SqlParameter("@TestNormalRangeFemale", obx.TestNormalRangeFemale));
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                newDataLoadValueErrorId = Convert.ToInt32(reader.GetValue(0));
            }

            reader.Close();

            return newDataLoadValueErrorId;
        }

        public void ResolveDataLoadValueError(int resolvedDataLoadValueErrorId)
        {
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            cmd.CommandText = "ResolveDataLoadValueError";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@DataLoadValueErrorId", resolvedDataLoadValueErrorId));
            reader = cmd.ExecuteReader();

            reader.Close();
        }
    }
}
