using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Services
{

    public class DBHelper
    {

       

        public static void CreateFTS5(string dbName)
        {
            using (var connection = new SqliteConnection("Data Source=Resources/"+dbName))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;

                    string columns = "gid UNINDEXED,IDOBJ_NAME,IDOBJ_CUSTOMID, IDOBJ_ALIAS";
                    command.CommandText = $"CREATE VIRTUAL TABLE idobj_text_fts5 USING FTS5({columns}, tokenize='{Config.Instance.FTSTokenizer}')";
                    command.ExecuteNonQuery();

                    transaction.Commit();
                }
                connection.Close();
            }
        }

        public static void CreateTestData(string dbName,TestDataSize size)
        {
          
            var service = new SQLiteDbService(dbName);



                List<InsertObject> inserts = createTestInserts(size);


         

                var updates = new List<UpdateObject>
                {
                    /*new UpdateObject
                   {
                       Gid = 1,
                       ColumnNames= new[]
                       {
                           "IDOBJ_NAME"
                       },
                       Values = new []
                       {
                           new UpdateValue { ColumnName = "IDOBJ_NAME", StrValue = "Hoy" },
                       }
                   }*/
                };

                var gidsForDelete = new List<long>
                {

                };

                string[] columnNames = new[] { "IDOBJ_NAME", "IDOBJ_CUSTOMID", "IDOBJ_ALIAS" };
                service.UpdateDatabase(inserts, updates, gidsForDelete, columnNames);
            }


        

        public static List<InsertObject> createTestInserts(TestDataSize size)
        {
            string[] sampleWords1 = new[] { "car", "truck", "van" };

            string[] sampleWords2 = new[] { "bush", "grass", "ball" };

            string[] sampleWords3 = new[] { "cable", "switch", "plug" };

            var inserts = new List<InsertObject>();

            int id_count = 0;

            if (size >= TestDataSize.SMALL)
            {
                //2,000,000 car
                //1,500,000 truck
                //500,000 van
                for (int i = 0; i <= 1000000; i++)
                {
                    inserts.Add(new InsertObject
                    {
                        Gid = id_count += 1,
                        Values = new[]
                        {
                            new InsertValue { ColumnName = "IDOBJ_NAME", StrValue = AddRandomLetters(sampleWords1[0]) },
                            new InsertValue { ColumnName = "IDOBJ_CUSTOMID", StrValue = AddRandomLetters(sampleWords1[0]) },
                            new InsertValue { ColumnName = "IDOBJ_ALIAS", StrValue = AddRandomLetters(sampleWords1[0]) }
                        }
                    });
                }

                for (int i = 0; i <= 1000000; i++)
                {
                    inserts.Add(new InsertObject
                    {
                        Gid = id_count += 1,
                        Values = new[]
                        {
                            new InsertValue { ColumnName = "IDOBJ_NAME", StrValue = AddRandomLetters(sampleWords1[1]) },
                            new InsertValue { ColumnName = "IDOBJ_CUSTOMID", StrValue = AddRandomLetters(sampleWords1[0]) },
                            new InsertValue { ColumnName = "IDOBJ_ALIAS", StrValue = AddRandomLetters(sampleWords1[1]) }
                        }
                    });
                }

                for (int i = 0; i <= 500000; i++)
                {
                    inserts.Add(new InsertObject
                    {
                        Gid = id_count += 1,
                        Values = new[]
                        {
                            new InsertValue { ColumnName = "IDOBJ_NAME", StrValue = AddRandomLetters(sampleWords1[2]) },
                            new InsertValue { ColumnName = "IDOBJ_CUSTOMID", StrValue = AddRandomLetters(sampleWords1[1]) },
                            new InsertValue { ColumnName = "IDOBJ_ALIAS", StrValue = AddRandomLetters(sampleWords1[2]) }
                        }
                    });
                }
            }

            if (size >= TestDataSize.MEDIUM)
            {
                //5,000,000 bush
                for (int i = 0; i <= 5000000; i++)
                {
                    inserts.Add(new InsertObject
                    {
                        Gid = id_count += 1,
                        Values = new[]
                        {
                            new InsertValue { ColumnName = "IDOBJ_NAME", StrValue = AddRandomLetters(sampleWords2[0]) },
                            new InsertValue { ColumnName = "IDOBJ_CUSTOMID", StrValue = AddRandomLetters(sampleWords2[0]) },
                            new InsertValue { ColumnName = "IDOBJ_ALIAS", StrValue = AddRandomLetters(sampleWords2[0]) }
                        }
                    });
                }
            }

            if (size >= TestDataSize.LARGE)
            {
                //10,000,000 grass
                for (int i = 0; i <= 10000000; i++)
                {
                    inserts.Add(new InsertObject
                    {
                        Gid = id_count += 1,
                        Values = new[]
                        {
                            new InsertValue { ColumnName = "IDOBJ_NAME", StrValue = AddRandomLetters(sampleWords2[1]) },
                            new InsertValue { ColumnName = "IDOBJ_CUSTOMID", StrValue = AddRandomLetters(sampleWords2[1]) },
                            new InsertValue { ColumnName = "IDOBJ_ALIAS", StrValue = AddRandomLetters(sampleWords2[1]) }
                        }
                    });
                }


            }

            return inserts;
        }

        public static string AddRandomLetters(string word, int maxWords = 5)
        {
            int maxPadding = 5;
            Random rng = new Random();
            List<string> words = [];
            words.Add(word);
            for (int i = 0; i < rng.Next(0, maxWords); i++)
            {
                words.Add(RandomLetters(rng.Next(1, maxPadding + 1)));
            }

            int n = words.Count;
            while (n > 1)
            {
                n--;
                int k = rng.Next(n + 1);
                (words[n], words[k]) = (words[k], words[n]);
            }

            return string.Join("_", words);

        }

        private static string RandomLetters(int length)
        {
            Random rng = new Random();
            string chars = "abcdefghijklmnopqrstuvwxyz";
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                sb.Append(chars[rng.Next(chars.Length)]);
            }
            return sb.ToString();
        }
    }
}
