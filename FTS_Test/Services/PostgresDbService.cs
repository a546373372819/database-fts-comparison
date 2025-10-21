using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using Microsoft.Data.Sqlite;
using MongoDB.Driver.Core.Configuration;
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



        public void UpdateFTS4Tables(
    List<InsertObject> inserts,
    List<UpdateObject> updates,
    List<long> gidsForDelete,
    string[] columnNames)
        {
            using var connection = new NpgsqlConnection(DatabaseSource);
            connection.Open();

            using var transaction = connection.BeginTransaction();

            try
            {
                if (inserts != null && inserts.Count > 0)
                {
                    foreach (var ins in inserts)
                    {
                        var sbCols = new StringBuilder("gid, ");
                        var sbVals = new StringBuilder("@gid, ");

                        for (int i = 0; i < columnNames.Length; i++)
                        {
                            sbCols.Append(columnNames[i]);
                            sbVals.Append($"@val{i}");

                            if (i < columnNames.Length - 1)
                            {
                                sbCols.Append(", ");
                                sbVals.Append(", ");
                            }
                        }

                        var sql = $"INSERT INTO public.idobj_text_fts5 ({sbCols}) VALUES ({sbVals});";
                        using var cmd = new NpgsqlCommand(sql, connection, transaction);

                        cmd.Parameters.AddWithValue("gid", ins.Gid);

                        for (int i = 0; i < columnNames.Length; i++)
                        {
                            var value = ins.Values[i].StrValue ?? (object)DBNull.Value;
                            cmd.Parameters.AddWithValue($"val{i}", value);
                        }

                        cmd.ExecuteNonQuery();
                    }
                }

                if (updates != null && updates.Count > 0)
                {
                    foreach (var upd in updates)
                    {
                        var sb = new StringBuilder();
                        sb.Append("UPDATE public.idobj_text_fts5 SET ");

                        for (int i = 0; i < columnNames.Length; i++)
                        {
                            sb.Append($"{columnNames[i]} = @val{i}");
                            if (i < columnNames.Length - 1)
                                sb.Append(", ");
                        }

                        sb.Append(" WHERE gid = @gid;");

                        using var cmd = new NpgsqlCommand(sb.ToString(), connection, transaction);

                        for (int i = 0; i < columnNames.Length; i++)
                        {
                            var value = upd.Values[i].StrValue ?? (object)DBNull.Value;
                            cmd.Parameters.AddWithValue($"val{i}", value);
                        }

                        cmd.Parameters.AddWithValue("gid", upd.Gid);
                        cmd.ExecuteNonQuery();
                    }
                }

                if (gidsForDelete != null && gidsForDelete.Count > 0)
                {
                    using var deleteCmd = new NpgsqlCommand(
                        "DELETE FROM public.idobj_text_fts5 WHERE gid = ANY(@gids);",
                        connection,
                        transaction
                    );
                    deleteCmd.Parameters.AddWithValue("gids", gidsForDelete);
                    deleteCmd.ExecuteNonQuery();
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
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

        public List<long> FullTextSearch(DMSType type, ModelCode property, string filter, FilterOperation operation, int returnValuesLimit, bool searchIndividualWords, bool orderByRelevance)
        {
            throw new NotImplementedException();
        }
    }
}
