using FTS_Test.Models.Enum;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Models.DB
{
    public class DBSearch
    {
        public DBSearch(TestDataSize dbSize ,ModelCode property, string searchInput, FilterOperation searchMethod, bool searchIndividuallWords)
        {
            DBSize= dbSize;
            Property = property;
            SearchInput = searchInput;
            SearchMethod = searchMethod;
            SearchIndividuallWords = searchIndividuallWords;
        }

        public TestDataSize DBSize { get; set; }
        public ModelCode Property { get; set; }

        public string SearchInput { get; set; }

        public FilterOperation SearchMethod { get; set; }

        public bool SearchIndividuallWords { get; set; }
    }
}
