using FTS_Test.Models.DB;
using FTS_Test.Models.Enum;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace FTS_Test.Services
{
    internal class SQLiteDbService:IDbService
    {
        string DatabaseSource;

        public SQLiteDbService(string db) 
        {
            DatabaseSource = db;
        }

        public void UpdateDatabase(List<InsertObject> inserts, List<UpdateObject> updates, List<long> gidsForDelete, string[] columnNames)
        {
            UpdateFTS4Tables(inserts,updates,gidsForDelete,columnNames);
            UpdateFTS5Tables(inserts, updates, gidsForDelete);
        }

        public void UpdateFTS4Tables(List<InsertObject> inserts, List<UpdateObject> updates, List<long> gidsForDelete, string[] columnNames)
        {

            var _connection = new SqliteConnection("Data Source=Resources/" + DatabaseSource);


            _connection.Open();

            using (var Transaction = _connection.BeginTransaction())
            {

                // creating insert command indexed
                string queryInsertIndexed = "INSERT INTO idobj_text_indexed (gid, version_id, type_id) VALUES (@gid, 0, @type_id)";

                // creating insert command fts
                string queryInsertFTS = $@"INSERT INTO idobj_text_fts ({string.Join(", ", columnNames.Select(col => $@"""{col.ToString()}"""))})
	            VALUES ({string.Join(", ", columnNames.Select(col => $@"@{col.ToString()}"))})";

                if (inserts.Count > 0)
                {
                    Dictionary<string, int> insertFTSParamNameToIndex = new Dictionary<string, int>();

                    using (SqliteCommand commandInsertIndexed = new SqliteCommand(queryInsertIndexed, _connection, Transaction))
                    {
                        commandInsertIndexed.Parameters.Add("@gid", SqliteType.Integer);
                        commandInsertIndexed.Parameters.Add("@type_id", SqliteType.Integer);

                        using (SqliteCommand commandInsertFTS = new SqliteCommand(queryInsertFTS, _connection, Transaction))
                        {
                            for (int i = 0; i < columnNames.Length; i++)
                            {
                                commandInsertFTS.Parameters.Add($"@{columnNames[i]}", SqliteType.Text);
                                insertFTSParamNameToIndex[columnNames[i]] = i;
                            }


                            foreach (var insert in inserts)
                            {
                                commandInsertIndexed.Parameters[0].Value = insert.Gid;
                                commandInsertIndexed.Parameters[1].Value = (ushort)ExtractTypeFromGlobalId(insert.Gid);

                                foreach (SqliteParameter param in commandInsertFTS.Parameters)
                                {
                                    param.Value = null;
                                }

                                foreach (var insertValue in insert.Values)
                                {
                                    commandInsertFTS.Parameters[insertFTSParamNameToIndex[insertValue.ColumnName]].Value = insertValue.StrValue;
                                }

                                commandInsertIndexed.ExecuteNonQuery();
                                commandInsertFTS.ExecuteNonQuery();
                            }
                        }
                    }
                }

                if (updates.Count > 0)
                {
                    Dictionary<string, int> updateFTSParamNameToIndex = new Dictionary<string, int>();
                    for (int i = 0; i < columnNames.Length; i++)
                    {
                        updateFTSParamNameToIndex[columnNames[i]] = i + 1;
                    }

                    Dictionary<long, long> updatedGidToRowId = new Dictionary<long, long>(updates.Count);

                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT gid, rowid FROM idobj_text_indexed WHERE gid IN (");
                    foreach (var item in GetAllGids(updates))
                    {
                        sb.Append(item).Append(',');
                    }

                    if (sb.Length >= 1)
                        sb.Length--;

                    sb.Append(")");
                    string querySelectIndexed = sb.ToString();

                    using (SqliteCommand commandSelectIndexed = new SqliteCommand(querySelectIndexed, _connection, Transaction))
                    {
                        using (SqliteDataReader reader = commandSelectIndexed.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                updatedGidToRowId[reader.GetInt64(0)] = reader.GetInt64(1);
                            }
                        }
                    }

                    foreach (var update in updates)
                    {
                        long rowId;
                        if (updatedGidToRowId.TryGetValue(update.Gid, out rowId))
                        {
                            string queryUpdateFTS = $@"UPDATE idobj_text_fts SET {string.Join(", ", update.ColumnNames.Select(col => $@"""{col.ToString()}"" = @{col.ToString()}"))} WHERE rowid = @rowId";

                            using (SqliteCommand commandUpdateFTS = new SqliteCommand(queryUpdateFTS, _connection, Transaction))
                            {
                                commandUpdateFTS.Parameters.Add($"@rowId", SqliteType.Integer);

                                for (int i = 0; i < update.ColumnNames.Length; i++)
                                {
                                    commandUpdateFTS.Parameters.Add($"@{columnNames[i]}", SqliteType.Text);
                                }

                                commandUpdateFTS.Parameters[0].Value = rowId;
                                foreach (var updateValue in update.Values)
                                {
                                    commandUpdateFTS.Parameters[updateFTSParamNameToIndex[updateValue.ColumnName]].Value = updateValue.StrValue;
                                }

                                commandUpdateFTS.ExecuteNonQuery();
                            }

                        }
                    }
                }


                if (gidsForDelete.Count > 0)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT rowid FROM idobj_text_indexed WHERE gid IN (");
                    foreach (var item in gidsForDelete)
                    {
                        sb.Append(item).Append(',');
                    }

                    if (sb.Length >= 1)
                        sb.Length--;

                    sb.Append(") ORDER BY rowid DESC");
                    string querySelectIndexed = sb.ToString();

                    int deletedRowsCount = 0;
                    using (SqliteCommand commandSelectIndexed = new SqliteCommand(querySelectIndexed, _connection, Transaction))
                    {
                        using (SqliteDataReader reader = commandSelectIndexed.ExecuteReader())
                        {
                            sb = new StringBuilder();
                            while (reader.Read())
                            {
                                sb.Append(reader[0].ToString()).Append(',');
                                deletedRowsCount++;
                            }
                        }
                    }

                    if (sb.Length >= 1)
                        sb.Length--;
                    string rows = sb.ToString();

                    if (rows.Length > 0)
                    {
                        sb = new StringBuilder();
                        sb.Append("DELETE FROM idobj_text_fts WHERE rowid IN (");
                        sb.Append(rows);
                        sb.Append(")");
                        string queryDeleteFTS = sb.ToString();

                        sb = new StringBuilder();
                        sb.Append("DELETE FROM idobj_text_indexed WHERE rowid IN (");
                        sb.Append(rows);
                        sb.Append(")");
                        string queryDeleteIndexed = sb.ToString();

                        using (SqliteCommand commandDeleteFTS = new SqliteCommand(queryDeleteFTS, _connection, Transaction))
                        {
                            long affectedRowsFTS = commandDeleteFTS.ExecuteNonQuery();
                        }

                        using (SqliteCommand commandDeleteIndexed = new SqliteCommand(queryDeleteIndexed, _connection, Transaction))
                        {
                            long affectedRowsIndexed = commandDeleteIndexed.ExecuteNonQuery();
                        }
                    }
                }

                Transaction.Commit();

                _connection.Close();

            }


        }

        public void UpdateFTS5Tables(List<InsertObject> inserts, List<UpdateObject> updates, List<long> gidsForDelete)
        {
            var _connection = new SqliteConnection("Data Source=Resources/" + DatabaseSource);
            _connection.Open();
            var columnNames = Config.Instance.FTS5ColumnNames;


            using (var Transaction = _connection.BeginTransaction())
            {


                // creating insert command fts
                string queryInsertFTS = $@"INSERT INTO idobj_text_fts5 ({string.Join(", ", Config.Instance.FTS5ColumnNames.Select(col => $@"""{col.ToString()}"""))})
	            VALUES ({string.Join(", ", Config.Instance.FTS5ColumnNames.Select(col => $@"@{col.ToString()}"))})";

                if (inserts.Count > 0)
                {
                    Dictionary<string, int> insertFTSParamNameToIndex = new Dictionary<string, int>();


                    using (SqliteCommand commandInsertFTS = new SqliteCommand(queryInsertFTS, _connection, Transaction))
                    {
                        for (int i = 0; i < columnNames.Length; i++)
                        {
                            commandInsertFTS.Parameters.Add($"@{columnNames[i]}", SqliteType.Text);
                            insertFTSParamNameToIndex[columnNames[i]] = i;

                        }


                        foreach (var insert in inserts)
                        {


                            foreach (SqliteParameter param in commandInsertFTS.Parameters)
                            {
                                param.Value = null;
                            }

                            commandInsertFTS.Parameters[insertFTSParamNameToIndex["gid"]].Value = insert.Gid;


                            foreach (var insertValue in insert.Values)
                            {
                                commandInsertFTS.Parameters[insertFTSParamNameToIndex[insertValue.ColumnName]].Value = insertValue.StrValue;
                            }

                            commandInsertFTS.ExecuteNonQuery();
                        }
                    }


                }


                Transaction.Commit();
            }

        }

        private List<long> GetAllGids(List<UpdateObject> updates)
        {
            List<long> gids = new List<long>();

            foreach (UpdateObject update in updates)
            {
                gids.Add(update.Gid);
            }

            return gids;
        }


        private int ExtractTypeFromGlobalId(long gid)
        {
            return (int)((gid >> 32) & 0xFFFF);
        }

        public List<long> FullTextSearch(DMSType type, ModelCode property, string filter, FilterOperation operation, int returnValuesLimit, bool searchIndividualWords, bool orderByRelevance)
        {
            //ModelCode ime kolone
            //DMSType type idobj_text_fts

            SqliteConnection connection = null;

            try
            {
                connection = new SqliteConnection("Data Source=Resources/" + DatabaseSource);
                connection.Open();

                StringBuilder queryAdditionalFilter = new StringBuilder();
                string queryTypeAdding = string.Empty;
                StringBuilder query = new StringBuilder();

                string[] filterTokens = filter.ToLower().Split(Config.Instance.Separators.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                List<long> globalIdsResult = new List<long>();

                if (!filterTokens.Any())
                {
                    return globalIdsResult;
                }

                using (SqliteCommand command = new SqliteCommand("", connection))
                {
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
				    FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.[rowid] = ITI.[rowid] 
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
				FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.[rowid] = ITI.[rowid] 
				WHERE ({queryMatchRange} MATCH @filter {queryTypeAdding})");
                        }
                        else
                        {
                            query.AppendLine($@"
				SELECT gid
				FROM idobj_text_fts ITF LEFT JOIN idobj_text_indexed ITI ON ITF.[rowid] = ITI.[rowid] 
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

                    try
                    {
                        using (SqliteDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                long globalId = Convert.ToInt64(reader["gid"]);
                                globalIdsResult.Add(globalId);
                            }

                            reader.Close();
                        }
                    }
                    catch
                    {
                        throw;
                    }


                }

                return globalIdsResult;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                connection.Close();
            }
        }


        public List<long> FullTextSearch5(DMSType type, ModelCode property, string filter, FilterOperation operation, int returnValuesLimit, bool searchIndividualWords, bool orderByRelevance)
        {
            //ModelCode ime kolone
            //DMSType type idobj_text_fts

            SqliteConnection connection = null;

            try
            {
                connection = new SqliteConnection("Data Source=Resources/" + DatabaseSource);
                connection.Open();

                StringBuilder queryAdditionalFilter = new StringBuilder();
                string queryTypeAdding = string.Empty;
                StringBuilder query = new StringBuilder();

                string[] filterTokens = filter.ToLower().Split(Config.Instance.Separators.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                List<long> globalIdsResult = new List<long>();

                if (!filterTokens.Any())
                {
                    return globalIdsResult;
                }

                using (SqliteCommand command = new SqliteCommand("", connection))
                {
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

                            //command.Parameters.AddWithValue("@filter", filter.Replace("%", "*"));
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
				    FROM idobj_text_fts5
				    WHERE ( {queryTypeAdding}  {queryAdditionalFilter})");
                    }
                    else
                    {


                        if (searchIndividualWords)
                        {
                            queryProperties = property == 0 ? string.Join(", ", Config.Instance.columnNames.Select(c => $@"""{c.ToString()}""")) : $@"""{property}""";
                            string queryMatchRange = property == 0 ? "idobj_text_fts5" : $@"""{property}""";

                            query.AppendLine($@"
				SELECT gid
				FROM idobj_text_fts5
				WHERE ({queryMatchRange} MATCH @filter {queryTypeAdding})");
                        }
                        else
                        {
                            query.AppendLine($@"
				SELECT gid
				FROM idobj_text_fts5
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

                    if (returnValuesLimit < 0)
                    {
                        finalQuery.Append($" LIMIT {returnValuesLimit}");
                    }


                    command.CommandText = finalQuery.ToString();

                    foreach (SqliteParameter param in command.Parameters)
                    {
                        string name = param.ParameterName;

                        // Handle nulls, strings, and other types
                        string valueStr = param.Value.ToString();

                        valueStr = valueStr.Replace('"', ' ');

                        // Replace all instances of the parameter name
                        command.CommandText = command.CommandText.Replace(name, '"' + valueStr + '"');
                    }

                    Console.WriteLine(command.CommandText);

                    try
                    {
                        using (SqliteDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                long globalId = Convert.ToInt64(reader["gid"]);
                                globalIdsResult.Add(globalId);
                            }

                            reader.Close();
                        }
                    }
                    catch
                    {
                        throw;
                    }


                }

                return globalIdsResult;
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                connection.Close();
            }
        }

    }

    // Additional methods for database operations can be added here


}

