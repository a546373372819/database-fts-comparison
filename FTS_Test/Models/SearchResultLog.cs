using FTS_Test.Models.DB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Models
{
    internal class SearchResultLog
    {
        public SearchResultLog(DBSearch dbSearch, double? fTS4AvgTimeMs, int? fTS4ResultCount, double? fTS5AvgTimeMs, int? fTS5ResultCount)
        {
            DbSearch = dbSearch;
            SqliteAvgTimeMs = fTS4AvgTimeMs;
            SqliteResultCount = fTS4ResultCount;
            PostgresAvgTimeMs = fTS5AvgTimeMs;
            PostgresResultCount = fTS5ResultCount;
            SqliteToPostgresTimeRatio = fTS4AvgTimeMs / fTS5AvgTimeMs;
        }

        public DBSearch DbSearch { get; set; }
        public double? SqliteAvgTimeMs { get; set; }
        public int? SqliteResultCount { get; set; }
        public double? PostgresAvgTimeMs { get; set; }
        public int? PostgresResultCount { get; set; }

        public double? SqliteToPostgresTimeRatio { get; set; }

    }
}

