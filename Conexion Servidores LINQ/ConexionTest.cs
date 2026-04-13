using Microsoft.Data.SqlClient;

namespace Conexion_Servidores_LINQ
{
    public class ConexionTest
    {
        public static async Task ProbarConexion(string connectionString)
        {
            try
            {
                using var connection = new SqlConnection(connectionString);
                Console.WriteLine("? Intentando conectar...");
                await connection.OpenAsync();
                Console.WriteLine("? ¡Conexión exitosa!");

                using var cmd = new SqlCommand("SELECT @@VERSION", connection);
                var version = await cmd.ExecuteScalarAsync();
                Console.WriteLine($"Versión de SQL Server: {version}");
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"? Error de conexión: {ex.Message}");
                Console.WriteLine($"Número de error: {ex.Number}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error: {ex.Message}");
            }
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            // Prueba de conexión
            Console.WriteLine("¿Deseas probar la conexión remota? (s/n): ");
            if (Console.ReadLine()?.ToLower() == "s")
            {
                Console.Write("Ingresa la cadena de conexión: ");
                string connStr = Console.ReadLine();
                await ConexionTest.ProbarConexion(connStr);
                Console.ReadKey();
            }
        }
    }
}
