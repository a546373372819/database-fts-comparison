using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test
{
    internal class Config
    {
        private static Config _instance = new Config(); // Change _instance to static  
        public static Config Instance => _instance;

        public string FTSTokenizer { get; } = "unicode61";
        public string Separators { get; } = " _-.,;";

        public string TokenChars { get; } = "@#$%{}()[]!?=+/^\\:'’<>"; // Default token characters  

        public bool FTSRemoveDiacritics { get; } = false; // Default value for removing diacritics  

        public string[] columnNames = { "IDOBJ_NAME", "IDOBJ_CUSTOMID", "IDOBJ_ALIAS" }; // Fix array initialization syntax  
        public string[] FTS5ColumnNames = { "gid","IDOBJ_NAME", "IDOBJ_CUSTOMID", "IDOBJ_ALIAS" }; // Fix array initialization syntax  

    }
}
