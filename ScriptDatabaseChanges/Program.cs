using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ScriptDatabaseChanges
{
    class Program
    {
        static int _counter = 0;
        private static object _lock = new object();
        private static StringBuilder _builder = new StringBuilder();

        private static string TableDiffFilePath { get; set; }

        static void Main(string[] args)
        {

            var parameters = ParseParameters(args);
            TableDiffFilePath = GetTableDiffUtilityPath();

            var tables = GetTableList(GetSourceDatabaseConnectionString(parameters));

            var totalCount = tables.Count;
            Parallel.ForEach(tables, t =>
            {
                var changeScript = GenerateChangeScript(t, parameters);
                AppendChangeScript(changeScript);

                Interlocked.Increment(ref _counter);
                Console.WriteLine($"Finised {_counter} of {totalCount}.");
            });

            var tablesWithUnsupportedColumns = GetUnsupportedColumns();
            var columnsToCheck = ParseUnsupportedColumns(tablesWithUnsupportedColumns,
                GetSourceDatabaseConnectionString(parameters));

            GenerateDataFilesForUnsupportedColumns(columnsToCheck, parameters);
            var completeChangeScript = _builder.ToString();
            File.WriteAllText("changeScript.sql", completeChangeScript);
        }

        private static void GenerateDataFilesForUnsupportedColumns(List<AdditionalTable> columnsToCheck,
            ProgramParameters parameters)
        {
            using (var conn = new SqlConnection(GetSourceDatabaseConnectionString(parameters)))
            {
                conn.Open();
                foreach (var table in columnsToCheck)
                {
                    var baseSql =
                        $@"SELECT
                            [S].{table.PrimaryKeyColumn}
                        FROM
                            [{parameters.DestinationDatabase}].{table.Name} [T]
                            INNER JOIN [{parameters.SourceDatabase}].{table.Name} [S] ON [T].{table.PrimaryKeyColumn} = [S].{table.PrimaryKeyColumn} ";

                    foreach (var column in table.AdditionalColumns)
                    {
                        string whereClause;
                        switch (column.DataType)
                        {
                            case SqlDbType.Xml:
                                whereClause = $"CAST([T].{column.Name} as varbinary(max)) != CAST([S].{column.Name} as varbinary(max))";
                                break;

                            default:
                                throw new Exception("Datatype is not implemented");
                        }

                        List<int> primaryKeys;
                        using (var sql = new SqlCommand($"{baseSql} WHERE {whereClause}", conn))
                        using (var reader = sql.ExecuteReader())
                            primaryKeys = reader.Select(r => r.GetInt32(0)).ToList();

                        if (primaryKeys == null || primaryKeys.Count == 0) continue;

                        foreach (var primaryKey in primaryKeys)
                        {
                            using (var sql = new SqlCommand($"SELECT {column.Name} FROM {table.Name} WHERE {table.PrimaryKeyColumn} = {primaryKey}", conn))
                            {
                                var columnValue = sql.ExecuteScalar();

                                var directory = $"{table.Name.Replace("[","").Replace("]","").Replace(".","/")}_{table.PrimaryKeyColumn}/{column.Name}_{column.DataType}";
                                var fileName = $"{directory}/{primaryKey}.data";
                                if (Directory.Exists(directory) == false)
                                    Directory.CreateDirectory(directory);

                                switch (column.DataType)
                                {
                                    case SqlDbType.Xml:
                                    case SqlDbType.VarChar:
                                    case SqlDbType.NVarChar:
                                        File.WriteAllText(fileName, (string)columnValue);
                                        break;

                                    default:
                                        throw new Exception("Datatype is not implemented");
                                }
                            }
                        }
                    }
                }
            }
        }


        private static List<AdditionalTable> ParseUnsupportedColumns(List<(string TableName, List<string> ColumnNames)> columnList
            , string connectionString)
        {

            var unsupportedColumns = new List<(string TableName, string ColumnName, string DataType, string PkColumn)>();
            const string unsupportedColumnQuery =
                @"SELECT 
                    '[' + [C].TABLE_SCHEMA + '].[' + [C].TABLE_NAME + ']' [TABLE_NAME], COLUMN_NAME, DATA_TYPE, [PkColumn]
                FROM 
                    INFORMATION_SCHEMA.COLUMNS [C] 
                    INNER JOIN INFORMATION_SCHEMA.TABLES [T] ON [C].TABLE_NAME = [T].TABLE_NAME 
					CROSS APPLY
						(SELECT        
							TOP 1 CCU.COLUMN_NAME [PkColumn]
						FROM	            
							INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN
							INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
						WHERE        
							TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.TABLE_NAME = [T].TABLE_NAME) [PK]
                    LEFT OUTER JOIN (SELECT OBJECT_NAME(object_id) [TName], [name] [CName] FROM sys.computed_columns) [Computed] ON [T].TABLE_NAME = [Computed].TName AND [C].COLUMN_NAME = [Computed].CName
                WHERE 
                    TABLE_TYPE = 'BASE TABLE'
                    AND 
                    (
                        (DATA_TYPE IN ('varchar', 'nvarchar', 'varbinary') AND CHARACTER_MAXIMUM_LENGTH = -1)
                        OR
                        DATA_TYPE IN ('text', 'ntext', 'image', 'timestamp', 'xml')
                    )
                    AND [Computed].CName IS NULL";

            using (var conn = new SqlConnection(connectionString))
            using (var sql = new SqlCommand(unsupportedColumnQuery, conn))
            {
                conn.Open();
                using (var reader = sql.ExecuteReader())
                    unsupportedColumns = reader.Select(r => (r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3))).ToList();
            }


            var additionalTables = new List<AdditionalTable>();
            foreach (var changedColumn in columnList)
            {
                var tableEntry = unsupportedColumns.FirstOrDefault(c =>
                    string.Equals(c.TableName, changedColumn.TableName, StringComparison.CurrentCultureIgnoreCase));
                if (string.IsNullOrWhiteSpace(tableEntry.TableName)) continue;

                var table = new AdditionalTable(tableEntry.TableName, tableEntry.PkColumn);
                foreach (var columnName in changedColumn.ColumnNames)
                {
                    var columnEntry = unsupportedColumns.FirstOrDefault(c => c.ColumnName == columnName);
                    if (string.IsNullOrWhiteSpace(columnEntry.ColumnName)) continue;

                    SqlDbType type;
                    switch (columnEntry.DataType)
                    {
                        case "image":
                            type = SqlDbType.Image;
                            break;

                        case "nvarchar":
                            type = SqlDbType.NVarChar;
                            break;

                        case "varbinary":
                            type = SqlDbType.VarBinary;
                            break;

                        case "varchar":
                            type = SqlDbType.VarChar;
                            break;

                        case "xml":
                            type = SqlDbType.Xml;
                            break;

                        case "text":
                            type = SqlDbType.Text;
                            break;

                        case "ntext":
                            type = SqlDbType.NText;
                            break;

                        case "timestamp":
                            type = SqlDbType.Timestamp;
                            break;

                        default:
                            throw new Exception("Datatype is not implemented");
                    }
                    table.AdditionalColumns.Add(new AdditionalColumn(columnName, type));
                }
                if (table.AdditionalColumns.Count == 0)
                    continue;

                additionalTables.Add(table);
            }

            return additionalTables;
        }

        private static List<(string TableName, List<string> ColumnNames)> GetUnsupportedColumns()
        {
            const string columnSearchStartString = "-- Column(s) ";
            const string columnSearchEndString = " are not included in";
            const string tableSearchStartString = "-- Table: ";

            var lines = _builder.ToString().Split(Environment.NewLine).ToList();
            var unsupportedTables = new List<(string TableName, List<string> columnNames)>();

            var index = -1;
            while (true)
            {
                index = lines.FindIndex(index + 1, s => s.StartsWith(columnSearchStartString));
                if (index <= 0) break;

                var columnLine = lines[index];
                var columnStartIndex = columnLine.IndexOf(columnSearchStartString);
                var columnEndIndex = columnLine.IndexOf(columnSearchEndString);
                if (columnStartIndex == -1 || columnEndIndex == -1)
                    continue;

                var columnList = columnLine
                    .Substring(columnStartIndex + columnSearchStartString.Length,
                        columnEndIndex - columnStartIndex - columnSearchStartString.Length)
                    .Split(",", StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .ToList();
                if (columnList.Count == 0)
                    continue;

                var tableLine = lines[index - 1];
                var tableStartIndex = tableLine.IndexOf(tableSearchStartString);
                if (tableStartIndex == -1)
                    continue;

                var tableName = tableLine.Substring(tableStartIndex + tableSearchStartString.Length).Trim();
                if (string.IsNullOrWhiteSpace(tableName))
                    continue;

                var table = (tableName, columnList);
                unsupportedTables.Add(table);
            }

            return unsupportedTables;
        }

        private static void AppendChangeScript(string changeScript)
        {
            if (string.IsNullOrWhiteSpace(changeScript)) return;

            lock (_lock)
            {
                _builder.AppendLine($"{changeScript}GO\r\n");
            }
        }

        public static string GetSourceDatabaseConnectionString(ProgramParameters parameters)
        {
            var connStringBuilder = new SqlConnectionStringBuilder
            {
                InitialCatalog = parameters.SourceDatabase,
                DataSource = parameters.Server,
                IntegratedSecurity = true
            };

            return connStringBuilder.ToString();
        }

        private static List<string> GetTableList(string connectionString)
        {
            List<string> tables = null;


            using (var conn = new SqlConnection(connectionString))
            using (var sql = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn))
            {
                conn.Open();

                using (var reader = sql.ExecuteReader())
                    tables = reader.Select(r => r.GetString(0)).ToList();

                sql.CommandText = "SELECT opKurokoMainSettings FROM Operator WHERE opid = 33";
                var result = sql.ExecuteScalar();
                var type = result.GetType();
            }

            tables.Remove("CheckDBLog");
            tables.Remove("EventLog");
            tables.Remove("DentalProcedureHistory");

            return tables;
        }

        private static string GenerateChangeScript(string table, ProgramParameters parameters)
        {
            var fileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var outputFile = $"{fileName}.sql";

            var diffCommandArguments =
                $"-sourceserver {parameters.Server} -sourcedatabase {parameters.SourceDatabase} -destinationserver {parameters.Server} -destinationdatabase {parameters.DestinationDatabase} -sourcetable {table} -destinationtable {table} -f \"{fileName}\"";

            var proc = Process.Start(@"C:\Program Files\Microsoft SQL Server\140\COM\tablediff.exe", diffCommandArguments);
            proc.WaitForExit();

            var changeScript = string.Empty;
            if (File.Exists(outputFile))
            {
                changeScript = File.ReadAllText(outputFile);
                File.Delete(outputFile);
            }
            return changeScript;
        }

        private static string GetTableDiffUtilityPath()
        {
            var baseFolders = new[]
                {@"C:\Program Files\Microsoft SQL Server", @"C:\Program Files (x86)\Microsoft SQL Server"};
            var versions = new[] { "140", "130", "120", "110", "100", "90", "80" };

            return baseFolders.SelectMany(b => versions.Select(v => $@"{b}\{v}\COM\tablediff.exe"))
                .Where(File.Exists)
                .First();
        }

        private static ProgramParameters ParseParameters(string[] args)
        {
            if (args == null || args.Length == 0) throw new Exception("Command line parameters must be used");
            if (args.Length % 2 != 0) throw new Exception("Invalid number of command line parameters");

            var parameters = new ProgramParameters();

            for (var i = 0; i < args.Length; i += 2)
            {
                var parameterName = args[i];
                var parameterValue = args[i + 1];

                switch (parameterName.ToLower().Replace("--", "").Replace("-", "").Replace("/", ""))
                {
                    case "server":
                        parameters.Server = parameterValue;
                        break;

                    case "sourcedatabase":
                        parameters.SourceDatabase = parameterValue;
                        break;

                    case "destinationdatabase":
                        parameters.DestinationDatabase = parameterValue;
                        break;

                    default:
                        throw new NotImplementedException($"{parameterName} not a valid parameter");
                }
            }

            return parameters;
        }
    }

    public class ProgramParameters
    {
        public string SourceDatabase { get; set; }
        public string Server { get; set; }
        public string DestinationDatabase { get; set; }
    }

    public struct AdditionalTable
    {
        public AdditionalTable(string name, string primaryKeyColumn)
        {
            Name = name;
            PrimaryKeyColumn = primaryKeyColumn;
            AdditionalColumns = new List<AdditionalColumn>();
        }

        public string Name { get; }
        public string PrimaryKeyColumn { get; }
        public List<AdditionalColumn> AdditionalColumns { get; }
    }

    public struct AdditionalColumn
    {
        public string Name { get; }
        public SqlDbType DataType { get; }

        public AdditionalColumn(string name, SqlDbType dataType)
        {
            Name = name;
            DataType = dataType;
        }
    }

    public static class Extensions
    {
        public static IEnumerable<T> Select<T>(this SqlDataReader reader, Func<SqlDataReader, T> projection)
        {
            while (reader.Read())
            {
                yield return projection(reader);
            }
        }
    }
}
