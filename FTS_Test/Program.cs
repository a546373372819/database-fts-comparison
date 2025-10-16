using ClosedXML.Excel;
using DocumentFormat.OpenXml.Drawing.Diagrams;
using FTS_Test.Models;
using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using FTS_Test.Services;
using Microsoft.Data.Sqlite;
using Npgsql;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection.Metadata; // Use Microsoft.Data.Sqlite for .NET Core compatibility
using System.Text;

namespace FTS_Test
{


    class Program
    {
        static void Main(string[] args)
        {

            //DBHelper.CreateFTS5("dms_model_big.db3");
            //DBHelper.CreateTestData("dms_model_big.db3", TestDataSize.LARGE);



            /*FTService ftService = new FTService();

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            var res = ftService.FullTextSearch(0, ModelCode.idobj_text_fts, "car sfs", FilterOperation.FTS, 100000000, true, false);
            Console.WriteLine(res.Count() + "," + stopwatch.Elapsed);

            stopwatch.Restart();
            var res2 = ftService.FullTextSearch5(0, ModelCode.idobj_text_fts, "car sfs", FilterOperation.FTS, 100000000, true, false);
            Console.WriteLine(res2.Count() + "," + stopwatch.Elapsed);


            WriteResults(res);
            Console.WriteLine("\n\n");
            WriteResults(res2);*/

            //RunSQLiteDBStressTest();
            RunDatabaseComparison();

            /*string connString = "Host=localhost;Port=2345;Username=postgres;Password=admin;Database=mydb";
            
            using var conn = new NpgsqlConnection(connString);
            conn.Open();

            // Query to get all tables in the 'public' schema
            string sql = @"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        ";

            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = cmd.ExecuteReader();

            Console.WriteLine("Tables in database:");
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            }*/

        }

        public static void WriteResults(List<long> res)
        {

            if (res.Count()<20)
            {
                foreach (var item in res)
                {
                    Console.WriteLine(item);
                }
            }
        }

        public static void RunDatabaseComparison()
        {
            List<SearchResultLog> results = new List<SearchResultLog>();
            List<DBSearch> DbSearches = CreateDbInputs();


            IDbService ftServicePostgresMedium = new PostgresDbService("Host=localhost;Port=2345;Username=postgres;Password=admin;Database=mydb;Pooling=true;MinPoolSize=1;MaxPoolSize=20;");
            IDbService ftServiceSqliteMedium = new SQLiteDbService("dms_model_medium.db3");


            foreach (DBSearch dbSearch in DbSearches)
            {

                switch (dbSearch.DBSize)
                {
                    case TestDataSize.SMALL:
                        continue;
                        break;
                    case TestDataSize.LARGE:
                        continue;
                        break;
                    default:
                        break;
                }

                double SqliteTimeAvg = 0.0;
                double PostgresTimeAvg = 0.0;

                int iterations = 10;
                double SqliteTime;
                double PostgresTime;


                List<long> SqliteRes = null;
                List<long> PostgresRes = null;


                for (int i = 0; i < iterations; i++)
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();

                    if (dbSearch.Property == ModelCode.search_vector)
                    {
                        dbSearch.Property = ModelCode.idobj_text_fts;
                    }
                    SqliteRes = ftServiceSqliteMedium.FullTextSearch(0, dbSearch.Property, dbSearch.SearchInput, dbSearch.SearchMethod, 100000000, dbSearch.SearchIndividuallWords, false);
                    SqliteTime = stopwatch.Elapsed.TotalMilliseconds;


                    stopwatch.Restart();

                    if(dbSearch.Property== ModelCode.idobj_text_fts)
                    {
                        dbSearch.Property = ModelCode.search_vector;
                    }
                
                    PostgresRes = ftServicePostgresMedium.FullTextSearch5(0, dbSearch.Property, dbSearch.SearchInput, dbSearch.SearchMethod, 100000000, dbSearch.SearchIndividuallWords, false);
                    PostgresTime = stopwatch.Elapsed.TotalMilliseconds;

                    SqliteTimeAvg += SqliteTime;
                    PostgresTimeAvg += PostgresTime;

  

                }


                SqliteTimeAvg /= iterations;
                PostgresTimeAvg /= iterations;

                Console.WriteLine("Fts4 time avg:" + SqliteTimeAvg);
                Console.WriteLine("Fts5 time avg:" + PostgresTimeAvg);



                SearchResultLog result = new SearchResultLog(dbSearch, SqliteTimeAvg, SqliteRes.Count(), PostgresTimeAvg, PostgresRes.Count());
                results.Add(result);

            }

            CreateExcel(results);

        }

        public static void RunSQLiteDBStressTest()
        {

            List<SearchResultLog> results = new List<SearchResultLog>();
            List<DBSearch> DbSearches = CreateDbInputs();

            IDbService ftServiceSmall = new SQLiteDbService("dms_model.db3");
            IDbService ftServiceMedium = new SQLiteDbService("dms_model_medium.db3");
            IDbService ftServiceLarge = new SQLiteDbService("dms_model_big.db3");


            foreach (DBSearch dbSearch in DbSearches)
            {
                IDbService ftsService =null;

                switch (dbSearch.DBSize)
                {
                    case TestDataSize.SMALL:
                        ftsService = ftServiceSmall;
                        break;
                    case TestDataSize.MEDIUM:
                        ftsService = ftServiceMedium;
                        break;
                    case TestDataSize.LARGE:
                        ftsService = ftServiceLarge;
                        break;
                }

                double Fts4TimeAvg=0.0;
                double Fts5TimeAvg=0.0;

                int iterations = 10;
                double Fts4Time;
                double Fts5Time;


                List<long> Fts5Res=null;
                List<long> Fts4Res = null;


                for (int i = 0; i < iterations; i++)
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    Fts4Res = ftsService.FullTextSearch(0, dbSearch.Property, dbSearch.SearchInput, dbSearch.SearchMethod, 100000000, dbSearch.SearchIndividuallWords, false);
                    Fts4Time = stopwatch.Elapsed.TotalMilliseconds;


                    stopwatch.Restart();

                    Fts5Res = ftsService.FullTextSearch5(0, dbSearch.Property, dbSearch.SearchInput, dbSearch.SearchMethod, 100000000, dbSearch.SearchIndividuallWords, false);
                    Fts5Time = stopwatch.Elapsed.TotalMilliseconds;
                    
                    Fts4TimeAvg += Fts4Time;
                    Fts5TimeAvg += Fts5Time;

                    Console.WriteLine("Iteration:" + i);
                    Console.WriteLine("results:" + Fts5Res.Count());
                    Console.WriteLine("Fts4 time:" + Fts4Time);
                    Console.WriteLine("Fts5 time:" + Fts5Time);

                }
                Console.WriteLine("10 iterations complete");
                Console.WriteLine("Fts4 total time:" + Fts4TimeAvg);
                Console.WriteLine("Fts5 total time:" + Fts5TimeAvg);

                Fts4TimeAvg /= iterations;
                Fts5TimeAvg/= iterations;

                Console.WriteLine("Fts4 time avg:" + Fts4TimeAvg);
                Console.WriteLine("Fts5 time avg:" + Fts5TimeAvg);



                SearchResultLog result = new SearchResultLog(dbSearch, Fts4TimeAvg, Fts4Res.Count(), Fts5TimeAvg, Fts5Res.Count());
                results.Add(result);

            }

            CreateExcel(results);


        }

        public static List<DBSearch> CreateDbInputs()
        {
            List<DBSearch> DbSearches = new List<DBSearch>();
            string[] FtsInputStringsSmall = { "car", "bahr", "truck", "van", "car sfs" };
            string[] LikeInputStringsSmall = { "*car*", "*bahr*", "truck*" };

            string[] FtsInputStringsMedium = { "car", "bahr", "truck", "van", "car sfs", "bush", };
            string[] LikeInputStringsMedium = { "*car*", "*bahr*", "truck*", "*bush*", };

            string[] FtsInputStringsBig = { "car", "bahr", "truck", "van", "car sfs", "bush","grass" };
            string[] LikeInputStringsBig = { "*car*", "*bahr*", "truck*", "*bush*","*grass*" };


            foreach (var inputString in FtsInputStringsSmall)
            {

                DbSearches.Add(new DBSearch(TestDataSize.SMALL, ModelCode.idobj_text_fts, inputString, FilterOperation.FTS, true));

            }

            foreach (var inputString in LikeInputStringsSmall)
            {

                DbSearches.Add(new DBSearch(TestDataSize.SMALL, ModelCode.idobj_name, inputString, FilterOperation.Like, true));

            }

            foreach (var inputString in FtsInputStringsMedium)
            {

                DbSearches.Add(new DBSearch(TestDataSize.MEDIUM, ModelCode.idobj_text_fts, inputString, FilterOperation.FTS, true));

            }

            foreach (var inputString in LikeInputStringsMedium)
            {

                DbSearches.Add(new DBSearch(TestDataSize.MEDIUM, ModelCode.idobj_name, inputString, FilterOperation.Like, true));

            }

            foreach (var inputString in FtsInputStringsBig)
            {

                DbSearches.Add(new DBSearch(TestDataSize.LARGE, ModelCode.idobj_text_fts, inputString, FilterOperation.FTS, true));

            }

            foreach (var inputString in LikeInputStringsBig)
            {

                DbSearches.Add(new DBSearch(TestDataSize.LARGE, ModelCode.idobj_name, inputString, FilterOperation.Like, true));

            }

            return DbSearches;

        }
        public static void CreateExcel(List<SearchResultLog> logs)
        {
            // After collecting logs
            var workbook = new XLWorkbook();
            var worksheet = workbook.Worksheets.Add("Search Results");


            // Headers

            var properties = typeof(SearchResultLog).GetProperties();
            var propertiesSearch = typeof(DBSearch).GetProperties();

            int ColCounter = 0;
            for (int col = 0; col < properties.Length; col++)
            {
                if (properties[col].PropertyType == typeof(DBSearch))
                {
                    for (int colSearch=0; colSearch < propertiesSearch.Length; colSearch++)
                    {
   
                        worksheet.Cell(1, ColCounter + 1).Value = propertiesSearch[colSearch].Name;
                        ColCounter++;
                    }
                }
                else
                {
                    worksheet.Cell(1, ColCounter + 1).Value = properties[col].Name;
                    ColCounter++;
                }
                    
            }

            // Fill data
            for (int i = 0; i < logs.Count; i++)
            {
                ColCounter = 0;
                for (int col = 0; col < properties.Length ; col++)
                {
                    if (properties[col].PropertyType == typeof(DBSearch))
                    {
                        for (int colSearch = 0; colSearch < propertiesSearch.Length; colSearch++)
                        {

                            worksheet.Cell(i+2, ColCounter +1).Value = propertiesSearch[colSearch].GetValue(logs[i].DbSearch).ToString();
                            ColCounter++;
                        }
                    }
                    else
                    {
                        worksheet.Cell(i + 2, ColCounter + 1).Value = properties[col].GetValue(logs[i]).ToString();
                        ColCounter++;

                    }
                }

            }
            worksheet.Columns().AdjustToContents();
            workbook.SaveAs("SearchBenchmark.xlsx");
        }

       
        

        
        


    }
}