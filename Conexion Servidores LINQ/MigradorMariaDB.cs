using MySqlConnector;
using System.Data;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos a MariaDB.
    /// Crea la base de datos y tabla dinámicas basadas en el DataTable.
    /// </summary>
    public class MigradorMariaDB(string connectionString)
    {
        private const string DATABASE_NAME = "ConexionSQL";
        private const string TABLE_NAME = "DatosImportados";

        /// <summary>
        /// Migra datos directamente desde un DataTable a MariaDB.
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
                var resultCrear = await CrearTablaDeMariaDB(dataTable);
                if (!resultCrear.Success)
                {
                    return (false, resultCrear.Message);
                }

                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int filasInsertadas = 0;
                int filasConError = 0;

                Console.WriteLine($"\n? Migrando {dataTable.Rows.Count} filas a MariaDB...\n");

                foreach (DataRow row in dataTable.Rows)
                {
                    try
                    {
                        var columnasComilladas = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select(c => $"`{c.ColumnName}`"));
                        var parametros = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select((c, i) => $"@p{i}"));

                        string insert = $"INSERT INTO `{TABLE_NAME}` ({columnasComilladas}) VALUES ({parametros})";

                        using var cmd = new MySqlCommand(insert, connection);
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

                string mensaje = $"Migración exitosa a MariaDB:\n" +
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
            catch (MySqlException ex) when (ex.Message.Contains("Unable to connect"))
            {
                return (false, "Error de conexión: No se pudo conectar a MariaDB.\nVerifica que el servidor esté en ejecución.");
            }
            catch (MySqlException ex) when (ex.Message.Contains("Access denied"))
            {
                return (false, "Error de autenticación: Credenciales incorrectas.\nVerifica el usuario y contraseña.");
            }
            catch (MySqlException ex)
            {
                return (false, $"Error de MariaDB: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error inesperado: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y tabla dinámica basada en el DataTable.
        /// </summary>
        private async Task<(bool Success, string Message)> CrearTablaDeMariaDB(DataTable dataTable)
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                // Crear base de datos si no existe
                string crearDB = $"CREATE DATABASE IF NOT EXISTS `{DATABASE_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";

                using (var cmd = new MySqlCommand(crearDB, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                connection.Close();

                using var connectionDB = new MySqlConnection(builder.ConnectionString);
                await connectionDB.OpenAsync();

                // Construir definición de columnas basada en DataTable
                var columnDefinitions = new List<string>();
                foreach (DataColumn col in dataTable.Columns)
                {
                    string sqlType = ObtenerTipoMariaDB(col.DataType);
                    columnDefinitions.Add($"`{col.ColumnName}` {sqlType}");
                }

                string columnList = string.Join(",\n                    ", columnDefinitions);

                // Crear tabla dinámica
                string crearTabla = $@"
                CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                    {columnList}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";

                using (var cmd = new MySqlCommand(crearTabla, connectionDB))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, $"Tabla dinámica '{TABLE_NAME}' creada correctamente con {dataTable.Columns.Count} columnas");
            }
            catch (MySqlException ex)
            {
                return (false, $"Error al crear tabla en MariaDB: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Mapea tipos de datos .NET a tipos MariaDB.
        /// </summary>
        private static string ObtenerTipoMariaDB(Type dotNetType)
        {
            return dotNetType switch
            {
                _ when dotNetType == typeof(int) => "INT",
                _ when dotNetType == typeof(long) => "BIGINT",
                _ when dotNetType == typeof(short) => "SMALLINT",
                _ when dotNetType == typeof(byte) => "TINYINT",
                _ when dotNetType == typeof(decimal) => "DECIMAL(18, 2)",
                _ when dotNetType == typeof(double) => "DOUBLE",
                _ when dotNetType == typeof(float) => "FLOAT",
                _ when dotNetType == typeof(bool) => "BOOLEAN",
                _ when dotNetType == typeof(DateTime) => "DATETIME",
                _ when dotNetType == typeof(Guid) => "CHAR(36)",
                _ => "VARCHAR(255)"
            };
        }

        /// <summary>
        /// Obtiene toda la tabla importada de MariaDB como DataTable completo.
        /// </summary>
        public async Task<(bool Success, DataTable Datos, string Message)> ObtenerTablaCompletaAsync()
        {
            try
            {
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT * FROM `{TABLE_NAME}`";

                var resultados = new DataTable();
                using var adapter = new MySqlDataAdapter(query, connection);
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
        public async Task<DataTable> ObtenerDatosDeMariaDB()
        {
            var resultados = new DataTable();

            try
            {
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT * FROM `{TABLE_NAME}`";

                using var adapter = new MySqlDataAdapter(query, connection);
                adapter.Fill(resultados);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener datos de MariaDB: {ex.Message}");
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
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT COUNT(*) as Total FROM `{TABLE_NAME}`";

                using var cmd = new MySqlCommand(query, connection);
                int totalFilas = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);

                // Obtener total de columnas
                query = $"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_NAME = '{TABLE_NAME}' AND TABLE_SCHEMA = '{DATABASE_NAME}'";

                using var cmd2 = new MySqlCommand(query, connection);
                int totalColumnas = Convert.ToInt32(await cmd2.ExecuteScalarAsync() ?? 0);

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
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME,
                    SslMode = MySqlSslMode.Disabled
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = $"TRUNCATE TABLE `{TABLE_NAME}`";

                using var cmd = new MySqlCommand(delete, connection);
                await cmd.ExecuteNonQueryAsync();

                return (true, "Tabla limpiada correctamente");
            }
            catch (Exception ex)
            {
                return (false, $"Error al limpiar tabla: {ex.Message}");
            }
        }

        /// <summary>
        /// Elimina la base de datos completa de MariaDB.
        /// </summary>
        public async Task<(bool Success, string Message)> EliminarBaseDatosAsync()
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                string delete = $"DROP DATABASE IF EXISTS `{DATABASE_NAME}`";

                using var cmd = new MySqlCommand(delete, connection);
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