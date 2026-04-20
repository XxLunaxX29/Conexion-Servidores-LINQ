using Microsoft.Data.SqlClient;
using System.Data;
using System.Net;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos a SQL Server.
    /// Crea la base de datos y tabla dinámicas basadas en el DataTable.
    /// </summary>
    public class MigradorSQL(string connectionString)
    {
        private const string DATABASE_NAME = "ConexionSQL";
        private const string TABLE_NAME = "DatosImportados";

        static MigradorSQL()
        {
            SqlConnection.ClearAllPools();
            ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        }

        /// <summary>
        /// Migra datos directamente desde un DataTable a SQL Server.
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
                var resultCrear = await CrearTablaDeSQL(dataTable);
                if (!resultCrear.Success)
                {
                    return (false, resultCrear.Message);
                }

                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = DATABASE_NAME,
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int filasInsertadas = 0;
                int filasConError = 0;

                Console.WriteLine($"\n⏳ Migrando {dataTable.Rows.Count} filas a SQL Server...\n");

                foreach (DataRow row in dataTable.Rows)
                {
                    try
                    {
                        var columnasConCorchetes = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select(c => $"[{c.ColumnName}]"));
                        var parametros = string.Join(", ",
                            dataTable.Columns.Cast<DataColumn>().Select(c => $"@{c.ColumnName}"));

                        string insert = $"INSERT INTO [{TABLE_NAME}] ({columnasConCorchetes}) VALUES ({parametros})";

                        using var cmd = new SqlCommand(insert, connection);
                        cmd.CommandTimeout = 30;

                        foreach (DataColumn col in dataTable.Columns)
                        {
                            var valor = row[col] ?? DBNull.Value;
                            cmd.Parameters.AddWithValue($"@{col.ColumnName}", valor);
                        }

                        await cmd.ExecuteNonQueryAsync();
                        filasInsertadas++;

                        if (filasInsertadas % 100 == 0)
                        {
                            Console.Write($"\r✓ Procesadas: {filasInsertadas}/{dataTable.Rows.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        filasConError++;
                        Console.Write($"\r⚠ Fila {filasInsertadas + filasConError}: {ex.Message.Substring(0, Math.Min(50, ex.Message.Length))}");
                    }
                }

                Console.WriteLine($"\r✓ Migración completada: {filasInsertadas} insertadas | {filasConError} con error");

                string mensaje = $"Migración exitosa a SQL Server:\n" +
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
            catch (SqlException ex) when (ex.Number == -1)
            {
                return (false, "Error de conexión: Timeout o instancia no encontrada.\nVerifica que SQL Server esté en ejecución.");
            }
            catch (SqlException ex) when (ex.Number == 18456)
            {
                return (false, "Error de autenticación: Credenciales incorrectas.\nVerifica el usuario y contraseña.");
            }
            catch (SqlException ex)
            {
                return (false, $"Error de SQL Server: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error inesperado: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y tabla dinámica basada en el DataTable.
        /// </summary>
        private async Task<(bool Success, string Message)> CrearTablaDeSQL(DataTable dataTable)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "master",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Crear base de datos
                string crearDB = $@"
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{DATABASE_NAME}')
                BEGIN
                    CREATE DATABASE [{DATABASE_NAME}];
                END";

                using (var cmd = new SqlCommand(crearDB, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                connection.ChangeDatabase(DATABASE_NAME);

                // Construir definición de columnas basada en DataTable
                var columnDefinitions = new List<string>();
                foreach (DataColumn col in dataTable.Columns)
                {
                    string sqlType = ObtenerTipoSQL(col.DataType);
                    columnDefinitions.Add($"[{col.ColumnName}] {sqlType}");
                }

                string columnList = string.Join(",\n                    ", columnDefinitions);

                // Crear tabla dinámica
                string crearTabla = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE name=N'{TABLE_NAME}' AND type='U')
                CREATE TABLE [{TABLE_NAME}] (
                    {columnList}
                )";

                using (var cmd = new SqlCommand(crearTabla, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, $"Tabla dinámica '{TABLE_NAME}' creada correctamente con {dataTable.Columns.Count} columnas");
            }
            catch (SqlException ex)
            {
                return (false, $"Error al crear tabla en SQL Server: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Mapea tipos de datos .NET a tipos SQL Server.
        /// </summary>
        private static string ObtenerTipoSQL(Type dotNetType)
        {
            return dotNetType switch
            {
                _ when dotNetType == typeof(int) => "INT",
                _ when dotNetType == typeof(long) => "BIGINT",
                _ when dotNetType == typeof(short) => "SMALLINT",
                _ when dotNetType == typeof(byte) => "TINYINT",
                _ when dotNetType == typeof(decimal) => "DECIMAL(18, 2)",
                _ when dotNetType == typeof(double) => "FLOAT",
                _ when dotNetType == typeof(float) => "REAL",
                _ when dotNetType == typeof(bool) => "BIT",
                _ when dotNetType == typeof(DateTime) => "DATETIME2",
                _ when dotNetType == typeof(Guid) => "UNIQUEIDENTIFIER",
                _ => "NVARCHAR(MAX)"
            };
        }

        /// <summary>
        /// Obtiene todos los datos de la tabla importada como DataTable.
        /// </summary>
        public async Task<DataTable> ObtenerDatosDeSQL()
        {
            var resultados = new DataTable();

            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = DATABASE_NAME,
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Verificar que la tabla existe
                string verificarTabla = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE name=N'{TABLE_NAME}' AND type='U')
                BEGIN
                    RAISERROR('La tabla {TABLE_NAME} no existe', 16, 1)
                END";

                using (var cmd = new SqlCommand(verificarTabla, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                string query = $"SELECT * FROM [{TABLE_NAME}] ORDER BY (SELECT NULL)";

                using var cmd2 = new SqlCommand(query, connection);
                cmd2.CommandTimeout = 60;

                using var adapter = new SqlDataAdapter(cmd2);
                adapter.Fill(resultados);

                if (resultados.Rows.Count == 0)
                {
                    Console.WriteLine($"⚠ La tabla '{TABLE_NAME}' existe pero está vacía.");
                }

                return resultados;
            }
            catch (SqlException ex) when (ex.Number == -1)
            {
                Console.WriteLine("✗ Error de conexión: Timeout o instancia no encontrada.");
                return resultados;
            }
            catch (SqlException ex) when (ex.Number == 18456)
            {
                Console.WriteLine("✗ Error de autenticación: Credenciales incorrectas.");
                return resultados;
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"✗ Error de SQL Server: {ex.Message}");
                return resultados;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error inesperado: {ex.Message}");
                return resultados;
            }
        }

        /// <summary>
        /// Obtiene estadísticas de la tabla importada.
        /// </summary>
        public async Task<(int TotalFilas, int TotalColumnas)> ObtenerEstadisticasAsync()
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = DATABASE_NAME,
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT COUNT(*) as Total FROM [{TABLE_NAME}]";

                using var cmd = new SqlCommand(query, connection);
                int totalFilas = (int)await cmd.ExecuteScalarAsync();

                // Obtener total de columnas
                query = $@"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{TABLE_NAME}' AND TABLE_SCHEMA = 'dbo'";

                using var cmd2 = new SqlCommand(query, connection);
                int totalColumnas = (int)await cmd2.ExecuteScalarAsync();

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
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = DATABASE_NAME,
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = $"TRUNCATE TABLE [{TABLE_NAME}]";

                using var cmd = new SqlCommand(delete, connection);
                await cmd.ExecuteNonQueryAsync();

                return (true, "Tabla limpiada correctamente");
            }
            catch (Exception ex)
            {
                return (false, $"Error al limpiar tabla: {ex.Message}");
            }
        }

        /// <summary>
        /// Elimina la base de datos completa de SQL Server.
        /// </summary>
        public async Task<(bool Success, string Message)> EliminarBaseDatosAsync()
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "master",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = $@"
                IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{DATABASE_NAME}')
                BEGIN
                    ALTER DATABASE [{DATABASE_NAME}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{DATABASE_NAME}];
                END";

                using var cmd = new SqlCommand(delete, connection);
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