using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Models.DB
{
    internal class UpdateValue
    {
        public UpdateValue(string columnName, string strValue)
        {
            ColumnName = columnName;
            StrValue = strValue;
        }
        public string ColumnName { get; set; }
        public string StrValue { get; set; }
    }
}
