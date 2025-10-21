using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test
{
    internal class Config
    {
        private static Config _instance = new Config(); 
        public static Config Instance => _instance;

        public string FTSTokenizer { get; } = "unicode61";
        public string Separators { get; } = " _-.,;";

        public string TokenChars { get; } = "@#$%{}()[]!?=+/^\\:'’<>"; 

        public bool FTSRemoveDiacritics { get; } = false; 

        public string[] columnNames = { "IDOBJ_NAME", "IDOBJ_CUSTOMID", "IDOBJ_ALIAS" }; 
        public string[] FTS5ColumnNames = { "gid","IDOBJ_NAME", "IDOBJ_CUSTOMID", "IDOBJ_ALIAS" }; 

    }
}
