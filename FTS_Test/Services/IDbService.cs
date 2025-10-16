using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Services
{
    internal interface IDbService
    {
        void UpdateDatabase(List<InsertObject> inserts, List<UpdateObject> updates, List<long> gidsForDelete, string[] columnNames);

        public List<long> FullTextSearch(DMSType type, ModelCode property, string filter, FilterOperation operation, int returnValuesLimit, bool searchIndividualWords, bool orderByRelevance);

        public List<long> FullTextSearch5(DMSType type, ModelCode property, string filter, FilterOperation operation, int returnValuesLimit, bool searchIndividualWords, bool orderByRelevance);

    }
}
