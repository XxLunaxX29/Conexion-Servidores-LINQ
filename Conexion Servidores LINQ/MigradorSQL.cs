using Conexion_Servidores_LINQ;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Net;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos de productos a SQL Server.
    /// Crea la base de datos, tabla y sincroniza datos extraídos.
    /// </summary>
    public class MigradorSQL(string connectionString)
    {
        static MigradorSQL()
        {
            // Ignorar validación de certificado SSL (solo para desarrollo)
            SqlConnection.ClearAllPools();
            ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        }

        /// <summary>
        /// Migra una lista de productos primordiales a SQL Server.
        /// </summary>
        public async Task<(bool Success, string Message)> MigrarProductosAsync(List<ProductoPrimordial> productos)
        {
            try
            {
                if (productos == null || productos.Count == 0)
                {
                    return (false, "La lista de productos está vacía.");
                }

                // Crear base de datos y tabla
                await CrearTablaEnSQL();

                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "ConexionSQL",
                    Encrypt = SqlConnectionEncryptOption.Optional, // O usar 'Mandatory' si SSL es obligatorio
                    TrustServerCertificate = true // Confiar en cualquier certificado (solo desarrollo)
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int productosInsertados = 0;
                int productosExistentes = 0;
                var errores = new List<string>();

                Console.WriteLine("\n⏳ Migrando productos a SQL Server...\n");

                foreach (var producto in productos)
                {
                    try
                    {
                        string insert = @"
                        IF NOT EXISTS (SELECT 1 FROM Producto WHERE Id = @Id)
                        BEGIN
                            INSERT INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                            VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)
                        END";

                        using var cmd = new SqlCommand(insert, connection);
                        cmd.Parameters.AddWithValue("@Id", producto.Id ?? "");
                        cmd.Parameters.AddWithValue("@Nombre", producto.Nombre ?? "");
                        cmd.Parameters.AddWithValue("@Categoria", producto.Categoria ?? "");
                        cmd.Parameters.AddWithValue("@Valor", producto.Valor);
                        cmd.Parameters.AddWithValue("@Cantidad", producto.Cantidad);
                        cmd.Parameters.AddWithValue("@PrecioUnitario", producto.PrecioUnitario);

                        int rowsAffected = await cmd.ExecuteNonQueryAsync();

                        if (rowsAffected > 0)
                        {
                            productosInsertados++;
                        }
                        else
                        {
                            productosExistentes++;
                        }

                        if ((productosInsertados + productosExistentes) % 100 == 0)
                        {
                            Console.Write($"\r✓ Procesados: {productosInsertados + productosExistentes}/{productos.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        errores.Add($"Producto {producto.Id}: {ex.Message}");
                    }
                }

                Console.WriteLine($"\r✓ Migracion completada: {productosInsertados} insertados | {productosExistentes} existentes");

                string mensaje = $"Migracion exitosa:\n" +
                    $"- Productos insertados: {productosInsertados}\n" +
                    $"- Productos existentes: {productosExistentes}";

                if (errores.Count > 0)
                {
                    mensaje += $"\n- Errores: {errores.Count}";
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
                return (false, $"Error de SQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Sincroniza un único producto a SQL Server.
        /// </summary>
        public async Task<(bool Success, string Message)> SincronizarProductoAsync(ProductoPrimordial producto)
        {
            try
            {
                if (string.IsNullOrEmpty(producto.Id))
                {
                    return (false, "El ID del producto no puede estar vacío.");
                }

                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "ConexionSQL",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string insert = @"
                IF NOT EXISTS (SELECT 1 FROM Producto WHERE Id = @Id)
                BEGIN
                    INSERT INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                    VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)
                END
                ELSE
                BEGIN
                    UPDATE Producto
                    SET Nombre = @Nombre, Categoria = @Categoria, Valor = @Valor, 
                        Cantidad = @Cantidad, PrecioUnitario = @PrecioUnitario
                    WHERE Id = @Id
                END";

                using var cmd = new SqlCommand(insert, connection);
                cmd.Parameters.AddWithValue("@Id", producto.Id);
                cmd.Parameters.AddWithValue("@Nombre", producto.Nombre ?? "");
                cmd.Parameters.AddWithValue("@Categoria", producto.Categoria ?? "");
                cmd.Parameters.AddWithValue("@Valor", producto.Valor);
                cmd.Parameters.AddWithValue("@Cantidad", producto.Cantidad);
                cmd.Parameters.AddWithValue("@PrecioUnitario", producto.PrecioUnitario);

                int rowsAffected = await cmd.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    return (true, $"✓ Producto {producto.Id} sincronizado correctamente");
                }
                else
                {
                    return (false, $"No se pudo sincronizar el producto {producto.Id}");
                }
            }
            catch (SqlException ex)
            {
                return (false, $"Error de SQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y la tabla de productos en SQL Server.
        /// </summary>
        public async Task<(bool Success, string Message)> CrearTablaEnSQL()
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
                string crearDB = @"
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ConexionSQL')
                BEGIN
                    CREATE DATABASE ConexionSQL;
                END";

                using (var cmd = new SqlCommand(crearDB, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                connection.ChangeDatabase("ConexionSQL");

                // Crear tabla de productos
                string crearTabla = @"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Producto' AND xtype='U')
                CREATE TABLE Producto (
                    Id NVARCHAR(50) PRIMARY KEY,
                    Nombre NVARCHAR(255) NOT NULL,
                    Categoria NVARCHAR(100),
                    Valor DECIMAL(18, 2) NOT NULL,
                    Cantidad INT NOT NULL,
                    PrecioUnitario DECIMAL(18, 2) NOT NULL,
                    FechaCreacion DATETIME DEFAULT GETDATE()
                )";

                using (var cmd = new SqlCommand(crearTabla, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, "Base de datos y tabla creadas correctamente");
            }
            catch (SqlException ex)
            {
                return (false, $"Error de SQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene todos los productos desde SQL Server.
        /// </summary>
        public async Task<List<ProductoPrimordial>> ObtenerProductosDeSQL()
        {
            var productos = new List<ProductoPrimordial>();

            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "ConexionSQL",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = "SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario FROM Producto ORDER BY Id";

                using var cmd = new SqlCommand(query, connection);
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    productos.Add(new ProductoPrimordial
                    {
                        Id = reader["Id"].ToString(),
                        Nombre = reader["Nombre"].ToString(),
                        Categoria = reader["Categoria"].ToString(),
                        Valor = Convert.ToDecimal(reader["Valor"]),
                        Cantidad = Convert.ToInt32(reader["Cantidad"]),
                        PrecioUnitario = Convert.ToDecimal(reader["PrecioUnitario"])
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener productos: {ex.Message}");
            }

            return productos;
        }

        /// <summary>
        /// Obtiene estadísticas de la tabla de productos.
        /// </summary>
        public async Task<(int Total, decimal ValorTotal, int CantidadTotal)> ObtenerEstadisticasAsync()
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "ConexionSQL",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = @"
                SELECT 
                    COUNT(*) as Total,
                    ISNULL(SUM(Valor), 0) as ValorTotal,
                    ISNULL(SUM(Cantidad), 0) as CantidadTotal
                FROM Producto";

                using var cmd = new SqlCommand(query, connection);
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    int total = Convert.ToInt32(reader["Total"]);
                    decimal valorTotal = Convert.ToDecimal(reader["ValorTotal"]);
                    int cantidadTotal = Convert.ToInt32(reader["CantidadTotal"]);

                    return (total, valorTotal, cantidadTotal);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener estadísticas: {ex.Message}");
            }

            return (0, 0, 0);
        }

        /// <summary>
        /// Elimina todos los datos de la tabla de productos.
        /// </summary>
        public async Task<(bool Success, string Message)> LimpiarTablaAsync()
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    InitialCatalog = "ConexionSQL",
                    Encrypt = SqlConnectionEncryptOption.Optional,
                    TrustServerCertificate = true
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = "TRUNCATE TABLE Producto";

                using var cmd = new SqlCommand(delete, connection);
                await cmd.ExecuteNonQueryAsync();

                return (true, "Tabla limpiada correctamente");
            }
            catch (Exception ex)
            {
                return (false, $"Error al limpiar tabla: {ex.Message}");
            }
        }
    }
}