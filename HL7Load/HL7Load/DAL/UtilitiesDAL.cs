using System;
using System.Data.SqlClient;

namespace HL7Load
{
    public class UtilitiesDAL
    {
        public static string ToNullableString(object datum)
        {
            if (datum is DBNull)
                return null;
            else
                return Convert.ToString(datum);
        }

        public static int? ToNullableInt(object datum)
        {
            if (datum is DBNull)
                return (int?)null;
            else
                return Convert.ToInt32(datum);
        }

        public static decimal? ToNullableDecimal(object datum)
        {
            if (datum is DBNull)
                return (decimal?)null;
            else
                return Convert.ToDecimal(datum);
        }

        public static DateTime? ToNullableDateTime(object datum)
        {
            if (datum is DBNull)
                return (DateTime?)null;
            else
                return Convert.ToDateTime(datum);
        }

        public static Guid? ToNullableGuid(object datum)
        {
            if (datum is DBNull)
                return (Guid?)null;
            else
                return (Guid?)datum;
        }

        public static Guid ToGuid(object datum)
        {
            return (Guid)datum;
        }

        public static string ToString(object datum)
        {
            if (datum is DBNull)
                return "";
            else
                return Convert.ToString(datum);
        }

        public static int ToInt(object datum)
        {
            if (datum is DBNull)
                return 0;
            else
                return Convert.ToInt32(datum);
        }

        public static bool ToBoolean(object datum)
        {
            return Convert.ToBoolean(datum);
        }

        public static DateTime ToDateTime(object datum)
        {
            return Convert.ToDateTime(datum);
        }

        // For all null parameters, use DBNull.Value in the stored procedure call
        public void MapNullParametersToDBNull(SqlCommand cmd)
        {
            foreach (SqlParameter parameter in cmd.Parameters)
            {
                if (parameter.Value == null)
                {
                    parameter.Value = DBNull.Value;
                }
            }
        }
    }
}
