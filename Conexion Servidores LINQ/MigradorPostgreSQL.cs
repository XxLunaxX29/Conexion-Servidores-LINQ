using Npgsql;
using System.Data;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos a PostgreSQL.
    /// Crea la base de datos y tabla dinámicas basadas en el DataTable.
    /// </summary>
    public class MigradorPostgreSQL(string connectionString)
    {
        private const string DATABASE_NAME = "ConexionPostgreSQL";
        private const string TABLE_NAME = "DatosImportados";

        /// <summary>
        /// Migra datos directamente desde un DataTable a PostgreSQL.
        /// Crea tabla dinámica basada en las columnas del DataTable.
        /// </summary>
        public async Task<(bool Success, string Message)> MigrarDataTableAsync(DataTable dataTable)
        {
            try
            {
                if (dataTable == null || dataTable.Rows.Count == 0)
                {
                    return (false, "El DataTable está vacío.");
                }

                // Crear base de datos y tabla dinámica
                var resultCrear = await CrearTablaDePostgreSQL(dataTable);
                if (!resultCrear.Success)
                {
                    return (false, resultCrear.Message);
                }

                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int filasInsertadas = 0;
                int filasConError = 0;

                Console.WriteLine($"\n? Migrando {dataTable.Rows.Count} filas a PostgreSQL...\n");

                foreach (DataRow row in dataTable.Rows)
                {
                    try
                    {
                        var columnasComilladas = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
                        var parametros = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select((c, i) => $"@p{i}"));

                        string insert = $"INSERT INTO \"{TABLE_NAME}\" ({columnasComilladas}) VALUES ({parametros})";

                        using var cmd = new NpgsqlCommand(insert, connection);
                        cmd.CommandTimeout = 30;

                        int paramIndex = 0;
                        foreach (DataColumn col in dataTable.Columns)
                        {
                            var valor = row[col] ?? DBNull.Value;
                            cmd.Parameters.AddWithValue($"@p{paramIndex}", valor);
                            paramIndex++;
                        }

                        await cmd.ExecuteNonQueryAsync();
                        filasInsertadas++;

                        if (filasInsertadas % 100 == 0)
                        {
                            Console.Write($"\r? Procesadas: {filasInsertadas}/{dataTable.Rows.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        filasConError++;
                        Console.Write($"\r? Fila {filasInsertadas + filasConError}: {ex.Message.Substring(0, Math.Min(50, ex.Message.Length))}");
                    }
                }

                Console.WriteLine($"\r? Migración completada: {filasInsertadas} insertadas | {filasConError} con error");

                string mensaje = $"Migración exitosa a PostgreSQL:\n" +
                    $"- Base de datos: {DATABASE_NAME}\n" +
                    $"- Tabla: {TABLE_NAME}\n" +
                    $"- Filas insertadas: {filasInsertadas}\n" +
                    $"- Columnas: {dataTable.Columns.Count}";

                if (filasConError > 0)
                {
                    mensaje += $"\n- Filas con error: {filasConError}";
                }

                return (true, mensaje);
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("could not connect"))
            {
                return (false, "Error de conexión: No se pudo conectar a PostgreSQL.\nVerifica que el servidor esté en ejecución.");
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("password") || ex.Message.Contains("authentication"))
            {
                return (false, "Error de autenticación: Credenciales incorrectas.\nVerifica el usuario y contraseña.");
            }
            catch (NpgsqlException ex)
            {
                return (false, $"Error de PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error inesperado: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y tabla dinámica basada en el DataTable.
        /// </summary>
        private async Task<(bool Success, string Message)> CrearTablaDePostgreSQL(DataTable dataTable)
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "postgres"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Crear base de datos si no existe
                string crearDB = $"CREATE DATABASE \"{DATABASE_NAME}\" WITH ENCODING 'UTF8'";

                try
                {
                    using var cmd = new NpgsqlCommand(crearDB, connection);
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (NpgsqlException ex) when (ex.SqlState == "42P04")
                {
                    // Base de datos ya existe
                }

                connection.Close();

                // Conectar a la nueva base de datos
                var builderDB = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connectionDB = new NpgsqlConnection(builderDB.ConnectionString);
                await connectionDB.OpenAsync();

                // Construir definición de columnas basada en DataTable
                var columnDefinitions = new List<string>();
                foreach (DataColumn col in dataTable.Columns)
                {
                    string pgType = ObtenerTipoPostgreSQL(col.DataType);
                    columnDefinitions.Add($"\"{col.ColumnName}\" {pgType}");
                }

                string columnList = string.Join(",\n                    ", columnDefinitions);

                // Crear tabla dinámica
                string crearTabla = $@"
                CREATE TABLE IF NOT EXISTS ""{TABLE_NAME}"" (
                    {columnList}
                )";

                using (var cmd = new NpgsqlCommand(crearTabla, connectionDB))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, $"Tabla dinámica '{TABLE_NAME}' creada correctamente con {dataTable.Columns.Count} columnas");
            }
            catch (NpgsqlException ex)
            {
                return (false, $"Error al crear tabla en PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Mapea tipos de datos .NET a tipos PostgreSQL.
        /// </summary>
        private static string ObtenerTipoPostgreSQL(Type dotNetType)
        {
            return dotNetType switch
            {
                _ when dotNetType == typeof(int) => "INTEGER",
                _ when dotNetType == typeof(long) => "BIGINT",
                _ when dotNetType == typeof(short) => "SMALLINT",
                _ when dotNetType == typeof(byte) => "SMALLINT",
                _ when dotNetType == typeof(decimal) => "NUMERIC(18, 2)",
                _ when dotNetType == typeof(double) => "DOUBLE PRECISION",
                _ when dotNetType == typeof(float) => "REAL",
                _ when dotNetType == typeof(bool) => "BOOLEAN",
                _ when dotNetType == typeof(DateTime) => "TIMESTAMP",
                _ when dotNetType == typeof(Guid) => "UUID",
                _ => "TEXT"
            };
        }

        /// <summary>
        /// Obtiene toda la tabla importada de PostgreSQL como DataTable completo.
        /// </summary>
        public async Task<(bool Success, DataTable Datos, string Message)> ObtenerTablaCompletaAsync()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT * FROM \"{TABLE_NAME}\" ORDER BY (SELECT NULL)";

                var resultados = new DataTable();
                using var adapter = new NpgsqlDataAdapter(query, connection);
                adapter.Fill(resultados);

                if (resultados.Rows.Count == 0)
                {
                    return (false, resultados, "No hay datos en la tabla importada.");
                }

                string mensaje = $"Tabla '{TABLE_NAME}' obtenida correctamente:\n" +
                    $"- Filas: {resultados.Rows.Count}\n" +
                    $"- Columnas: {resultados.Columns.Count}";

                return (true, resultados, mensaje);
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error al obtener tabla: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene todos los datos de la tabla importada como DataTable.
        /// </summary>
        public async Task<DataTable> ObtenerDatosDePostgreSQL()
        {
            var resultados = new DataTable();

            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT * FROM \"{TABLE_NAME}\"";

                using var adapter = new NpgsqlDataAdapter(query, connection);
                adapter.Fill(resultados);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener datos de PostgreSQL: {ex.Message}");
            }

            return resultados;
        }

        /// <summary>
        /// Obtiene estadísticas de la tabla importada.
        /// </summary>
        public async Task<(int TotalFilas, int TotalColumnas)> ObtenerEstadisticasAsync()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT COUNT(*) as Total FROM \"{TABLE_NAME}\"";

                using var cmd = new NpgsqlCommand(query, connection);
                int totalFilas = (int)(await cmd.ExecuteScalarAsync() ?? 0);

                // Obtener total de columnas
                query = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{TABLE_NAME}' AND table_schema = 'public'";

                using var cmd2 = new NpgsqlCommand(query, connection);
                int totalColumnas = (int)(await cmd2.ExecuteScalarAsync() ?? 0);

                return (totalFilas, totalColumnas);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener estadísticas: {ex.Message}");
                return (0, 0);
            }
        }

        /// <summary>
        /// Elimina todos los datos de la tabla importada.
        /// </summary>
        public async Task<(bool Success, string Message)> LimpiarTablaAsync()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = $"TRUNCATE TABLE \"{TABLE_NAME}\"";

                using var cmd = new NpgsqlCommand(delete, connection);
                await cmd.ExecuteNonQueryAsync();

                return (true, "Tabla limpiada correctamente");
            }
            catch (Exception ex)
            {
                return (false, $"Error al limpiar tabla: {ex.Message}");
            }
        }

        /// <summary>
        /// Elimina la base de datos completa de PostgreSQL.
        /// </summary>
        public async Task<(bool Success, string Message)> EliminarBaseDatosAsync()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "postgres"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = $"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{DATABASE_NAME}' AND pid <> pg_backend_pid(); DROP DATABASE IF EXISTS \"{DATABASE_NAME}\"";

                using var cmd = new NpgsqlCommand(delete, connection);
                cmd.CommandTimeout = 60;
                await cmd.ExecuteNonQueryAsync();

                return (true, $"Base de datos '{DATABASE_NAME}' eliminada correctamente");
            }
            catch (Exception ex)
            {
                return (false, $"Error al eliminar base de datos: {ex.Message}");
            }
        }
    }
}