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
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection sqlConnectionHL7 = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlConnectionHL7"].ToString());
            SqlConnection sqlConnectionMain = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlConnectionMain"].ToString());

            sqlConnectionHL7.Open();
            sqlConnectionMain.Open();

            // Get a list of the observations to be processed
            List<Observation> obxList = GetObservationList(sqlConnectionHL7, sqlConnectionMain);

            sqlConnectionHL7.Close();
            sqlConnectionMain.Close();
        }

        private static List<Observation> GetObservationList(SqlConnection sqlConnectionHL7, SqlConnection sqlConnectionMain)
        {
            List<Observation> obxList = new List<Observation>();

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = sqlConnectionHL7;

            // Get previously processed observations that failed
            cmd.CommandText = "GetMessageObservationsFailed";

            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Observation obx = new Observation();
                obx.MessageId = (string)reader.GetValue(0);
                obx.ObservationId = (string)reader.GetValue(1);
                obx.ObservationIdText = (string)reader.GetValue(2);
                obx.ResultData = (string)reader.GetValue(3);
                obx.ResultUnits = (string)reader.GetValue(4);
                obx.ReferenceRange = (string)reader.GetValue(5);
                obx.TestId = (int)reader.GetValue(6);
                obxList.Add(obx);
            }
            reader.Close();

            // Get new never-before-processed observations
            cmd.CommandText = "GetMessageObservationsUnattempted";
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Observation obx = new Observation();
                obx.MessageId = UtilitiesDAL.ToString(reader.GetValue(0));
                obx.ObservationId = UtilitiesDAL.ToString(reader.GetValue(1));
                obx.ObservationIdText = UtilitiesDAL.ToString(reader.GetValue(2));
                obx.ResultData = UtilitiesDAL.ToString(reader.GetValue(3));
                obx.ResultUnits = UtilitiesDAL.ToString(reader.GetValue(4));
                obx.ReferenceRange = UtilitiesDAL.ToString(reader.GetValue(5));
                obx.TestId = (int)reader.GetValue(6);
                obxList.Add(obx);
            }
            reader.Close();

            UpdateTestIds(obxList, sqlConnectionHL7, sqlConnectionMain);

            return obxList;
        }

        private static void UpdateTestIds(List<Observation> obxList, SqlConnection sqlConnectionHL7, SqlConnection sqlConnectionMain)
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
                        currentTestId = InsertNewTest(obx, sqlConnectionMain);
                        InsertNewObxToTestMapping(obx, sqlConnectionHL7);
                    }
                    obxIdDictionary[currentObxId] = currentTestId;
                }
            }
        }

        private static int InsertNewTest(Observation obx, SqlConnection sqlConnectionMain)
        {
            int newTestId = 0;

            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = sqlConnectionMain;

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

        private static void InsertNewObxToTestMapping(Observation obx, SqlConnection sqlConnectionHL7)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = sqlConnectionHL7;

            // Create a new mapping from the current observation's ObxId to the appropriate TestId
            cmd.CommandText = "InsertNewObxToTestMapping";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new SqlParameter("@ObservationId", obx.ObservationId));
            cmd.Parameters.Add(new SqlParameter("@TestId", obx.TestId));
            cmd.ExecuteNonQuery();
        }
    }
}
