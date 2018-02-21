using System;
using System.Collections.Generic;
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

            var tables = GetTableList(parameters);

            var totalCount = tables.Count;
            Parallel.ForEach(tables, t =>
            {
                var changeScript = GenerateChangeScript(t, parameters);
                AppendChangeScript(changeScript);

                Interlocked.Increment(ref _counter);
                Console.WriteLine($"Finised {_counter} of {totalCount}.");
            });

            var completeChangeScript = _builder.ToString();
            File.WriteAllText("changeScript.sql", completeChangeScript);
        }

        private static void AppendChangeScript(string changeScript)
        {
            if (string.IsNullOrWhiteSpace(changeScript)) return;

            lock (_lock)
            {
                _builder.AppendLine($"{changeScript}GO\r\n");
            }
        }

        private static List<string> GetTableList(ProgramParameters parameters)
        {
            List<string> tables = null;
            var connStringBuilder = new SqlConnectionStringBuilder
            {
                InitialCatalog = parameters.SourceDatabase,
                DataSource = parameters.SourceServer,
                IntegratedSecurity = true
            };

            using (var conn = new SqlConnection(connStringBuilder.ToString()))
            using (var sql = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn))
            {
                conn.Open();

                using (var reader = sql.ExecuteReader())
                    tables = reader.Select(r => r.GetString(0)).ToList();
            }

            tables.Remove("CheckDBLog");
            tables.Remove("EventLog");

            return tables;
        }

        private static string GenerateChangeScript(string table, ProgramParameters parameters)
        {
            var fileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var outputFile = $"{fileName}.sql";

            var diffCommandArguments =
                $"-sourceserver {parameters.SourceServer} -sourcedatabase {parameters.SourceDatabase} -destinationserver {parameters.DestinationServer} -destinationdatabase {parameters.DestinationDatabase} -sourcetable {table} -destinationtable {table} -f \"{fileName}\"";

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
                    case "sourceserver":
                        parameters.SourceServer = parameterValue;
                        break;

                    case "sourcedatabase":
                        parameters.SourceDatabase = parameterValue;
                        break;

                    case "destinationserver":
                        parameters.DestinationServer = parameterValue;
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
        public string SourceServer { get; set; }
        public string DestinationDatabase { get; set; }
        public string DestinationServer { get; set; }
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
