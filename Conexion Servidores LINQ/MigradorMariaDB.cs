using Conexion_Servidores_LINQ;
using MySqlConnector;
using System.Data;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos de productos a MariaDB.
    /// Crea la base de datos, tabla y sincroniza datos extraídos.
    /// </summary>
    public class MigradorMariaDB(string connectionString)
    {
        private const string BaseDatos = "ConexionSQL";

        /// <summary>
        /// Migra una lista de productos primordiales a MariaDB.
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
                await CrearTablaEnMariaDB();

                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = BaseDatos,
                    SslMode = MySqlSslMode.None
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int productosInsertados = 0;
                int productosExistentes = 0;
                var errores = new List<string>();

                Console.WriteLine("\n? Migrando productos a MariaDB...\n");

                foreach (var producto in productos)
                {
                    try
                    {
                        string insert = @"
                        INSERT IGNORE INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                        VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)";

                        using var cmd = new MySqlCommand(insert, connection);
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
                            Console.Write($"\r? Procesados: {productosInsertados + productosExistentes}/{productos.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        errores.Add($"Producto {producto.Id}: {ex.Message}");
                    }
                }

                Console.WriteLine($"\r? Migración completada: {productosInsertados} insertados | {productosExistentes} existentes");

                string mensaje = $"Migración exitosa:\n" +
                    $"- Productos insertados: {productosInsertados}\n" +
                    $"- Productos existentes: {productosExistentes}";

                if (errores.Count > 0)
                {
                    mensaje += $"\n- Errores: {errores.Count}";
                }

                return (true, mensaje);
            }
            catch (MySqlException ex)
            {
                return (false, $"Error de MariaDB: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Sincroniza un único producto a MariaDB.
        /// </summary>
        public async Task<(bool Success, string Message)> SincronizarProductoAsync(ProductoPrimordial producto)
        {
            try
            {
                if (string.IsNullOrEmpty(producto.Id))
                {
                    return (false, "El ID del producto no puede estar vacío.");
                }

                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = BaseDatos,
                    SslMode = MySqlSslMode.None
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string insert = @"
                INSERT INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)
                ON DUPLICATE KEY UPDATE
                    Nombre = @Nombre,
                    Categoria = @Categoria,
                    Valor = @Valor,
                    Cantidad = @Cantidad,
                    PrecioUnitario = @PrecioUnitario";

                using var cmd = new MySqlCommand(insert, connection);
                cmd.Parameters.AddWithValue("@Id", producto.Id);
                cmd.Parameters.AddWithValue("@Nombre", producto.Nombre ?? "");
                cmd.Parameters.AddWithValue("@Categoria", producto.Categoria ?? "");
                cmd.Parameters.AddWithValue("@Valor", producto.Valor);
                cmd.Parameters.AddWithValue("@Cantidad", producto.Cantidad);
                cmd.Parameters.AddWithValue("@PrecioUnitario", producto.PrecioUnitario);

                int rowsAffected = await cmd.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    return (true, $"? Producto {producto.Id} sincronizado correctamente");
                }
                else
                {
                    return (false, $"No se pudo sincronizar el producto {producto.Id}");
                }
            }
            catch (MySqlException ex)
            {
                return (false, $"Error de MariaDB: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y la tabla de productos en MariaDB.
        /// </summary>
        public async Task<(bool Success, string Message)> CrearTablaEnMariaDB()
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                // Crear base de datos
                string crearDB = $"CREATE DATABASE IF NOT EXISTS `{BaseDatos}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";

                using (var cmd = new MySqlCommand(crearDB, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                connection.ChangeDatabase(BaseDatos);

                // Crear tabla de productos
                string crearTabla = @"
                CREATE TABLE IF NOT EXISTS Producto (
                    Id VARCHAR(50) PRIMARY KEY,
                    Nombre VARCHAR(255) NOT NULL,
                    Categoria VARCHAR(100),
                    Valor DECIMAL(18, 2) NOT NULL,
                    Cantidad INT NOT NULL,
                    PrecioUnitario DECIMAL(18, 2) NOT NULL,
                    FechaCreacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_categoria (Categoria),
                    INDEX idx_nombre (Nombre)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";

                using (var cmd = new MySqlCommand(crearTabla, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, "Base de datos y tabla creadas correctamente");
            }
            catch (MySqlException ex)
            {
                return (false, $"Error de MariaDB: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene todos los productos desde MariaDB.
        /// </summary>
        public async Task<List<ProductoPrimordial>> ObtenerProductosDeMariaDB()
        {
            var productos = new List<ProductoPrimordial>();

            try
            {
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = BaseDatos,
                    SslMode = MySqlSslMode.None
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = "SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario FROM Producto ORDER BY Id";

                using var cmd = new MySqlCommand(query, connection);
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
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = BaseDatos,
                    SslMode = MySqlSslMode.None
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = @"
                SELECT 
                    COUNT(*) as Total,
                    COALESCE(SUM(Valor), 0) as ValorTotal,
                    COALESCE(SUM(Cantidad), 0) as CantidadTotal
                FROM Producto";

                using var cmd = new MySqlCommand(query, connection);
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
                var builder = new MySqlConnectionStringBuilder(connectionString)
                {
                    Database = BaseDatos,
                    SslMode = MySqlSslMode.None
                };

                using var connection = new MySqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = "TRUNCATE TABLE Producto";

                using var cmd = new MySqlCommand(delete, connection);
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