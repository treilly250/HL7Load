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
        private static SqlConnection SqlConnectionHL7 = null;
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

            SqlConnectionHL7 = new SqlConnection(settings.SqlConnectionHL7String);
            SqlConnectionMain = new SqlConnection(settings.SqlConnectionMainString);

            SqlConnectionHL7.Open();
            SqlConnectionMain.Open();

            FindUsersBySSN = settings.FindUsersBySSN;
        }

        public List<Observation> GetObservationList()
        {
            List<Observation> obxList = new List<Observation>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionHL7;

            // Get previously processed observations that failed
            cmd.CommandText = "GetMessageObservationsFailed";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Observation obx = new Observation();
                obx.MessageId = UtilitiesDAL.ToString(reader["MessageID"]);
                obx.SequenceId = UtilitiesDAL.ToInt(reader["SequenceID"]);
                obx.ObservationId = UtilitiesDAL.ToString(reader["OBXID"]);
                obx.ObservationIdText = UtilitiesDAL.ToString(reader["OBXIDTEXT"]);
                obx.ResultData = UtilitiesDAL.ToString(reader["OBXRESULT"]);
                obx.ResultUnits = UtilitiesDAL.ToString(reader["OBXUNITS"]);
                obx.ReferenceRange = UtilitiesDAL.ToString(reader["OBXREFRANGE"]);
                obx.TestId = UtilitiesDAL.ToInt(reader["TestID"]);
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
                obx.ObservationId = UtilitiesDAL.ToString(reader["OBXID"]);
                obx.ObservationIdText = UtilitiesDAL.ToString(reader["OBXIDTEXT"]);
                obx.ResultData = UtilitiesDAL.ToString(reader["OBXRESULT"]);
                obx.ResultUnits = UtilitiesDAL.ToString(reader["OBXUNITS"]);
                obx.ReferenceRange = UtilitiesDAL.ToString(reader["OBXREFRANGE"]);
                obx.TestId = UtilitiesDAL.ToInt(reader["TestID"]);
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
                SpecialObxSetup(obx, reader);
                obxList.Add(obx);
            }
            reader.Close();

            return obxList;
        }

        public List<PatientIdentification> GetAutoMatchMappings()
        {
            List<PatientIdentification> patientIdList = new List<PatientIdentification>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Get previously defined patient ID mappings
            cmd.CommandText = "GetAutoMatchMappings";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                PatientIdentification patientId = new PatientIdentification();
                patientId.LastName = UtilitiesDAL.ToString(reader["LastName"]);
                patientId.FirstName = UtilitiesDAL.ToString(reader["FirstName"]);
                patientId.Gender = UtilitiesDAL.ToString(reader["Gender"]);
                patientId.DOB = UtilitiesDAL.ToString(reader["DateOfBirth"]);
                patientId.SSN = UtilitiesDAL.ToString(reader["SSN"]);
                patientId.MatchedToUserId = UtilitiesDAL.ToInt(reader["MatchedToUserID"]);
                patientIdList.Add(patientId);
            }
            reader.Close();

            return patientIdList;
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

        public void UpdateProcessedObservationStatus(Observation obx, bool success, string message)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionHL7;

            // Add a new test result
            cmd.CommandText = "UpdateProcessedObservationStatus";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@MessageID", obx.MessageId));
            cmd.Parameters.Add(new SqlParameter("@SequenceID", obx.SequenceId));
            cmd.Parameters.Add(new SqlParameter("@Success", success));
            cmd.Parameters.Add(new SqlParameter("@Message", message));
            cmd.Parameters.Add(new SqlParameter("@DataLoadMismatchTestID", success ? (object)DBNull.Value : RecordMismatchForLaterResolution(obx)));
            cmd.ExecuteNonQuery();
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

            if (FindUsersBySSN)
            {
                GetUserIdBasedOnSSN(obx);
            }
            else
            {
                GetUserIdBasedOnUserCN(obx);
            }
        }

        private void GetUserIdBasedOnUserCN(Observation obx)
        {
            if (UserCNCache.Keys.Contains(obx.UserCN))
            {
                Logger.LogLine("Found " + obx.UserCN + " in the cache");
                obx.UserId = UserCNCache[obx.UserCN];
            }
            else
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
                int censusUserId = 0;
                string censusGender = "";
                DateTime censusDOB = new DateTime();

                SqlCommand cmd = new SqlCommand();
                SqlDataReader reader;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Connection = SqlConnectionMain;

                // Get the matching UserId for the SSN in the current observation
                cmd.CommandText = "GetUserIdBasedOnSSN";
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

        private int RecordMismatchForLaterResolution(Observation obx)
        {
            int mismatchId = GetExistingDataLoadMismatch(obx);
            if (mismatchId == 0)
            {
                mismatchId = InsertDataLoadMismatch(obx);
            }
            return InsertDataLoadMismatchTest(obx, mismatchId);
        }

        private int GetExistingDataLoadMismatch(Observation obx)
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

        private int InsertDataLoadMismatch(Observation obx)
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
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                newDataLoadMismatchId = Convert.ToInt32(reader.GetValue(0));
            }

            reader.Close();

            return newDataLoadMismatchId;
        }

        private int InsertDataLoadMismatchTest(Observation obx, int dataLoadMismatchId)
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

        public void UpdateUserScores(int userId)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Update this user's scores
            cmd.CommandText = "UpdateUserScores";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@UserId", userId));
            cmd.ExecuteNonQuery();
        }
    }
}
