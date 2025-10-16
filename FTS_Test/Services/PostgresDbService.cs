using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using Microsoft.Data.Sqlite;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Services
{
    internal class PostgresDbService : IDbService
    {
        string DatabaseSource;

        public PostgresDbService(string databaseSource)
        {
            DatabaseSource = databaseSource;
        }

        public List<long> FullTextSearch(
    DMSType type,
    ModelCode property,
    string filter,
    FilterOperation operation,
    int returnValuesLimit,
    bool searchIndividualWords,
    bool orderByRelevance)
        {
            using var connection = new NpgsqlConnection(DatabaseSource);
            connection.Open();

            List<long> globalIdsResult = new List<long>();
            string[] filterTokens = filter.ToLower()
                .Split(Config.Instance.Separators.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            if (!filterTokens.Any())
                return globalIdsResult;

            using var command = new NpgsqlCommand();
            command.Connection = connection;

            StringBuilder queryAdditionalFilter = new StringBuilder();
            StringBuilder query = new StringBuilder();
            string column = property == 0 ? "idobj_text_fts" : property.ToString(); // pick column name

            // Type filter
            string queryTypeAdding = type != 0 ? $"AND type_id = {(ushort)type}" : string.Empty;

            switch (operation)
            {
                case FilterOperation.Eq:
                    queryAdditionalFilter.Append($@" AND ""{property}"" = @filterEq COLLATE NOCASE");

                    StringBuilder queryMatchEqual = new StringBuilder();
                    queryMatchEqual.Append(string.Join(" AND ", filterTokens.Select(t => $"\"{t}\"")));
                    command.Parameters.AddWithValue("@filter", queryMatchEqual);
                    command.Parameters.AddWithValue("@filterEq", filter);

                    break;
                case FilterOperation.Like:
                case FilterOperation.LikeMC:
                    queryAdditionalFilter.Append($@"""{property}"" LIKE @filterLike");

                    command.Parameters.AddWithValue("@filter", filter.Replace("%", "*"));
                    command.Parameters.AddWithValue("@filterLike", filter.Replace("*", "%"));
                    break;
                default:
                    if (searchIndividualWords)
                    {
                        StringBuilder queryMatchDefault = new StringBuilder();

                        queryMatchDefault.Append(string.Join(" AND ", filterTokens.Select(t => $"\"{t}\"")));
                        queryMatchDefault.Insert(queryMatchDefault.Length - 1, "*");

                        command.Parameters.AddWithValue("@filter", queryMatchDefault.ToString());
                    }
                    else
                    {
                        command.Parameters.AddWithValue("@filter", filter + "*");
                    }
                    break;
            }



            if (type != 0)
            {
                queryTypeAdding = $"AND type_id = {(ushort)type}";
            }


            string queryProperties = string.Empty;

            if (operation == FilterOperation.Like || operation == FilterOperation.LikeMC)
            {
                query.AppendLine($@"
				    SELECT gid
				    FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.rowid = ITI.rowid 
				    WHERE ( {queryTypeAdding}  {queryAdditionalFilter})");
            }
            else
                    {


                if (searchIndividualWords)
                {
                    queryProperties = property == 0 ? string.Join(", ", Config.Instance.columnNames.Select(c => $@"""{c.ToString()}""")) : $@"""{property}""";
                    string queryMatchRange = property == 0 ? "idobj_text_fts" : $@"""{property}""";

                    query.AppendLine($@"
				SELECT gid
				FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.rowid = ITI.rowid 
				WHERE ({queryMatchRange} MATCH @filter {queryTypeAdding})");
                }
                else
                {
                    query.AppendLine($@"
				SELECT gid
				FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.rowid = ITI.rowid 
				WHERE (""{property}"" MATCH @filter {queryTypeAdding} {queryAdditionalFilter})");
                }
            }


            if (orderByRelevance)
            {
                List<string> filterTokensParams = new List<string>(filterTokens.Length);

                for (int i = 0; i < filterTokens.Length; i++)
                {
                    string filterTokenI = "@filterTokens" + i;

                    filterTokensParams.Add(filterTokenI);
                    command.Parameters.AddWithValue(filterTokenI, filterTokens[i]);
                }

                if (searchIndividualWords)
                {
                    query.AppendLine($@"ORDER BY rank({1}, {filterTokens.Length}, {string.Join(", ", filterTokensParams)}, {queryProperties}) DESC");
                }
                else
                {
                    query.AppendLine($@"ORDER BY rank({1}, {filterTokens.Length}, {string.Join(", ", filterTokensParams)}, ""{property}"") DESC");
                }
            }


            StringBuilder finalQuery = new StringBuilder();

            if (operation == FilterOperation.LikeMC)
            {
                finalQuery.Append("PRAGMA case_sensitive_like = TRUE; ");
            }

            else if (operation == FilterOperation.Like)
            {
                finalQuery.Append("PRAGMA case_sensitive_like = FALSE; ");
            }

            finalQuery.Append(query);

            if (returnValuesLimit > 0)
            {
                finalQuery.Append($" LIMIT {returnValuesLimit}");
            }


            command.CommandText = finalQuery.ToString();

            Console.WriteLine(command.CommandText);


            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                long globalId = reader.GetInt64(reader.GetOrdinal("gid"));
                globalIdsResult.Add(globalId);
            }

            return globalIdsResult;
        }


        public List<long> FullTextSearch5(
    DMSType type,
    ModelCode property,
    string filter,
    FilterOperation operation,
    int returnValuesLimit,
    bool searchIndividualWords,
    bool orderByRelevance)
        {
            // 1️⃣ Open a connection using 'using' so it's properly disposed
            using var connection = new NpgsqlConnection(DatabaseSource);
            connection.Open();

            List<long> globalIdsResult = new List<long>();

            // 2️⃣ Split the filter into tokens (words)
            string[] filterTokens = filter.ToLower()
                .Split(Config.Instance.Separators.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            if (!filterTokens.Any())
                return globalIdsResult;

            using var command = new NpgsqlCommand();
            command.Connection = connection;

            // 3️⃣ Additional filter for type
            string typeFilter = type != 0 ? $"AND type_id = {(ushort)type}" : string.Empty;

            // 4️⃣ Build the main WHERE clause depending on operation
            StringBuilder whereClause = new StringBuilder();
            switch (operation)
            {
                case FilterOperation.Eq:
                    // Exact match (case-insensitive)
                    whereClause.Append($@" ""{property}"" = @filterEq COLLATE " + "\"C\""); // Postgres uses COLLATE "C" for byte-wise case-insensitive
                    command.Parameters.AddWithValue("@filterEq", filter);
                    break;

                case FilterOperation.Like:
                case FilterOperation.LikeMC:
                    // LIKE match (case-sensitive or insensitive)
                    whereClause.Append($@" ""{property}"" LIKE @filterLike");
                    string likeValue = filter.Replace("*", "%");
                    command.Parameters.AddWithValue("@filterLike", likeValue);
                    break;

                default:
                    // FTS match
                    string ftsQuery;
                    if (searchIndividualWords)
                    {
                        // Join individual words with & (AND) operator
                        ftsQuery = string.Join(" & ", filterTokens.Select(t => t + ":*")); // prefix search
                    }
                    else
                    {
                        // Treat the whole filter as one phrase
                        ftsQuery = filter + ":*";
                    }
                    whereClause.Append($@" ""{property}"" @@ to_tsquery('simple', @filter)");
                    command.Parameters.AddWithValue("@filter", ftsQuery);
                    break;
            }

            // 5️⃣ Construct final query
            StringBuilder query = new StringBuilder();
            query.AppendLine($"SELECT gid FROM idobj_text_fts5 WHERE ({whereClause} {typeFilter})");

            // 6️⃣ Add ordering by relevance if requested
            if (orderByRelevance && operation != FilterOperation.Like && operation != FilterOperation.LikeMC)
            {
                query.AppendLine($"ORDER BY ts_rank_cd(\"{property}\", to_tsquery('simple', @filter)) DESC");
            }

            // 7️⃣ Add limit if requested
            if (returnValuesLimit > 0)
            {
                query.AppendLine($"LIMIT {returnValuesLimit}");
            }

            command.CommandText = query.ToString();
            Console.WriteLine(command.CommandText);

            // 8️⃣ Execute query
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                globalIdsResult.Add(reader.GetInt64(reader.GetOrdinal("gid")));
            }

            return globalIdsResult;
        }



        public void UpdateDatabase(List<InsertObject> inserts, List<UpdateObject> updates, List<long> gidsForDelete, string[] columnNames)
        {
            throw new NotImplementedException();
        }
    }
}
