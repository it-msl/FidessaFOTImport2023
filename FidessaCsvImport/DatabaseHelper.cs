using System;

namespace FidessaCsvImport
{
    public static class DatabaseHelper
    {
        public static object SafeParameter(object value)
        {
            if (value == null || value is string && value.ToString() == string.Empty)
            {
                return DBNull.Value;
            }
            return value;
        }
    }
}