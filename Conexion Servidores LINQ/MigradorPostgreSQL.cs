using Conexion_Servidores_LINQ;
using Npgsql;
using System.Data;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para migrar datos de productos a PostgreSQL.
    /// Crea la base de datos, tabla y sincroniza datos extraídos.
    /// </summary>
    public class MigradorPostgreSQL(string connectionString)
    {
        /// <summary>
        /// Migra una lista de productos primordiales a PostgreSQL.
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
                await CrearTablaEnPostgreSQL();

                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "ConexionPostgreSQL"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                int productosInsertados = 0;
                int productosExistentes = 0;
                var errores = new List<string>();

                Console.WriteLine("\n? Migrando productos a PostgreSQL...\n");

                foreach (var producto in productos)
                {
                    try
                    {
                        string insert = @"
                        INSERT INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                        VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)
                        ON CONFLICT (Id) DO NOTHING";

                        using var cmd = new NpgsqlCommand(insert, connection);
                        cmd.CommandTimeout = 30;

                        cmd.Parameters.AddWithValue("@Id", (object)producto.Id ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@Nombre", (object)producto.Nombre ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@Categoria", (object)producto.Categoria ?? DBNull.Value);
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
                        if (errores.Count > 20)
                        {
                            errores.Add("... (más errores)");
                            break;
                        }
                    }
                }

                Console.WriteLine($"\r? Migracion completada: {productosInsertados} insertados | {productosExistentes} existentes");

                string mensaje = $"Migracion exitosa:\n" +
                    $"- Productos insertados: {productosInsertados}\n" +
                    $"- Productos existentes: {productosExistentes}";

                if (errores.Count > 0)
                {
                    mensaje += $"\n- Errores: {errores.Count}\n";
                    for (int i = 0; i < Math.Min(5, errores.Count); i++)
                    {
                        mensaje += $"  • {errores[i]}\n";
                    }
                }

                return (true, mensaje);
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("could not connect"))
            {
                return (false, "Error de conexión: No se pudo conectar a PostgreSQL.\nVerifica que el servidor esté en ejecución.");
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("password"))
            {
                return (false, "Error de autenticación: Credenciales incorrectas.\nVerifica el usuario y contraseña.");
            }
            catch (NpgsqlException ex)
            {
                return (false, $"Error de PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Sincroniza un único producto a PostgreSQL.
        /// </summary>
        public async Task<(bool Success, string Message)> SincronizarProductoAsync(ProductoPrimordial producto)
        {
            try
            {
                if (string.IsNullOrEmpty(producto.Id))
                {
                    return (false, "El ID del producto no puede estar vacío.");
                }

                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "ConexionPostgreSQL"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string insert = @"
                INSERT INTO Producto (Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario)
                VALUES (@Id, @Nombre, @Categoria, @Valor, @Cantidad, @PrecioUnitario)
                ON CONFLICT (Id) DO UPDATE
                SET Nombre = @Nombre, Categoria = @Categoria, Valor = @Valor, 
                    Cantidad = @Cantidad, PrecioUnitario = @PrecioUnitario
                WHERE Producto.Id = @Id";

                using var cmd = new NpgsqlCommand(insert, connection);
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
            catch (NpgsqlException ex)
            {
                return (false, $"Error de PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Crea la base de datos y la tabla de productos en PostgreSQL.
        /// </summary>
        public async Task<(bool Success, string Message)> CrearTablaEnPostgreSQL()
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
                string crearDB = "CREATE DATABASE \"ConexionPostgreSQL\" WITH ENCODING 'UTF8'";

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
                    Database = "ConexionPostgreSQL"
                };

                using var connectionDB = new NpgsqlConnection(builderDB.ConnectionString);
                await connectionDB.OpenAsync();

                // Crear tabla de productos
                string crearTabla = @"
                CREATE TABLE IF NOT EXISTS Producto (
                    Id VARCHAR(50) PRIMARY KEY,
                    Nombre VARCHAR(255) NOT NULL,
                    Categoria VARCHAR(100),
                    Valor NUMERIC(18, 2) NOT NULL,
                    Cantidad INTEGER NOT NULL,
                    PrecioUnitario NUMERIC(18, 2) NOT NULL,
                    FechaCreacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )";

                using (var cmd = new NpgsqlCommand(crearTabla, connectionDB))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                return (true, "Base de datos y tabla creadas correctamente");
            }
            catch (NpgsqlException ex)
            {
                return (false, $"Error de PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene todos los productos desde PostgreSQL.
        /// </summary>
        public async Task<List<ProductoPrimordial>> ObtenerProductosDePostgreSQL()
        {
            var productos = new List<ProductoPrimordial>();

            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "ConexionPostgreSQL"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = "SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario FROM Producto ORDER BY Id";

                using var cmd = new NpgsqlCommand(query, connection);
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
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "ConexionPostgreSQL"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = @"
                SELECT 
                    COUNT(*) as Total,
                    COALESCE(SUM(Valor), 0) as ValorTotal,
                    COALESCE(SUM(Cantidad), 0) as CantidadTotal
                FROM Producto";

                using var cmd = new NpgsqlCommand(query, connection);
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
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = "ConexionPostgreSQL"
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string delete = "TRUNCATE TABLE Producto";

                using var cmd = new NpgsqlCommand(delete, connection);
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