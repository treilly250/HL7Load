using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HL7Load
{
    class DAL
    {
        private static SqlConnection SqlConnectionHL7 = null;
        private static SqlConnection SqlConnectionMain = null;
        private Dictionary<string, int> UserIdCache = new Dictionary<string, int>();
        string[] DateFormats =
                {
                "yyyyMMdd",
                "yyyyMMddHHmmss",
                "yyyy-MM-dd"
                };

        public DAL(string connectionStringHL7, string connectionStringMain)
        {
            SqlConnectionHL7 = new SqlConnection(connectionStringHL7);
            SqlConnectionMain = new SqlConnection(connectionStringMain);

            SqlConnectionHL7.Open();
            SqlConnectionMain.Open();
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
                obx.Sex = UtilitiesDAL.ToString(reader["PIDSEX"]);
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
                obx.Sex = UtilitiesDAL.ToString(reader["PIDSEX"]);
                obx.StreetAddress = UtilitiesDAL.ToString(reader["PIDSTREET"]);
                obx.OtherAddress = UtilitiesDAL.ToString(reader["PIDOTHER"]);
                obx.City = UtilitiesDAL.ToString(reader["PIDCITY"]);
                obx.State = UtilitiesDAL.ToString(reader["PIDSTATE"]);
                obx.Zip = UtilitiesDAL.ToString(reader["PIDZIP"]);
                SpecialObxSetup(obx, reader);
                obxList.Add(obx);
            }
            reader.Close();

            // Update observation/test link tables, as necessary, for incoming
            // observations that we've not seen before (OBXes with TestID = 0)
            UpdateTestIds(obxList);

            return obxList;
        }

        public void InsertNewTestResult(Observation obx)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

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
            cmd.ExecuteNonQuery();
        }

        private string GetSSN(SqlDataReader reader)
        {
            // Do this because the SSN embedded in an undefined component of
            // the Patient ID (Internal) field contains a tilde that ends the
            // interpretation of the contents of this field #3 component
            string ssn = "";

            string fullField = UtilitiesDAL.ToString(reader["PIDINTERNALPATIENTIDFULLFIELD"]);
            string[] component = fullField.Split('^');
            if (component.Length > 4)  // Fail if fewer than 5 components are found
            {
                string component5 = component[4];  // Grab component 5
                int tildeOffset = component5.IndexOf('~');
                if (tildeOffset >= 0 && tildeOffset < component5.Length)
                {
                    ssn = component5.Substring(tildeOffset + 1);
                }
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

            GetUserIdBasedOnUserCN(obx);
        }

        private void GetUserIdBasedOnUserCN(Observation obx)
        {
            if (UserIdCache.Keys.Contains(obx.UserCN))
            {
                Console.WriteLine("Found " + obx.UserCN + " in the cache");
                obx.UserId = UserIdCache[obx.UserCN];
            }
            else
            {
                int censusUserId = 0;
                string censusSex = "";
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
                    censusSex = UtilitiesDAL.ToString(reader["Gender"]);
                    censusDOB = UtilitiesDAL.ToDateTime(reader["DOB"]);
                }
                obx.UserId = censusUserId;
                UserIdCache[obx.UserCN] = censusUserId;
                Console.WriteLine("Added " + obx.UserCN + " to the cache");
                if (censusUserId > 0
                    &&
                    !MatchOnSexAndDOB(obx, censusSex, censusDOB))
                {
                    obx.UserId = 0;  // Treat as if user was not found, due to gender/DOB mismatch
                }

                reader.Close();
            }
        }

        private void UpdateTestIds(List<Observation> obxList)
        {
            Dictionary<string, int> obxIdDictionary = new Dictionary<string, int>();
            foreach (Observation obx in obxList)
            {
                int currentTestId = obx.TestId;
                string currentObxId = obx.ObservationId;
                if (obxIdDictionary.Keys.Contains(currentObxId))
                {
                    if (currentTestId == 0)
                    {
                        obx.TestId = obxIdDictionary[currentObxId];
                    }
                }
                else
                {
                    if (currentTestId == 0)
                    {
                        currentTestId = InsertNewTest(obx);
                        InsertNewObxToTestMapping(obx);
                    }
                    obxIdDictionary[currentObxId] = currentTestId;
                }
            }
        }

        private int InsertNewTest(Observation obx)
        {
            int newTestId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionMain;

            // Create a new test for the current observation OBXID
            cmd.CommandText = "InsertNewTest";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@Name", "(" + obx.ObservationId + ") " + obx.ObservationIdText));
            cmd.Parameters.Add(new SqlParameter("@NormalRange", obx.ReferenceRange + obx.ResultUnits));
            reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                newTestId = Convert.ToInt32(reader.GetValue(0));
                obx.TestId = newTestId;
            }

            reader.Close();

            return newTestId;
        }

        private void InsertNewObxToTestMapping(Observation obx)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = SqlConnectionHL7;

            // Create a new mapping from the current observation's ObxId to the appropriate TestId
            cmd.CommandText = "InsertNewObxToTestMapping";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@ObservationId", obx.ObservationId));
            cmd.Parameters.Add(new SqlParameter("@TestId", obx.TestId));
            cmd.ExecuteNonQuery();
        }

        private bool MatchOnSexAndDOB(Observation obx, string censusSex, DateTime censusDOB)
        {
            DateTime obxDOB;

            if (DateTime.TryParseExact(obx.DOB, DateFormats, new CultureInfo("en-US"), DateTimeStyles.None, out obxDOB))
            {
                if (censusDOB.Date == obxDOB
                    &&
                    censusSex.ToLower() == obx.Sex.ToLower())
                {
                    return true;
                }
            }
            return false;
        }
    }
}
