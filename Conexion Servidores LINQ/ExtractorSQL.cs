using Microsoft.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Net;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para extraer datos de SQL Server y mostrarlos en tabla en consola.
    /// </summary>
    public class ExtractorSQL(string connectionString)
    {
        static ExtractorSQL()
        {
            // Ignorar validación de certificado SSL (solo para desarrollo)
            ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        }

        /// <summary>
        /// Obtiene todos los productos desde SQL Server como DataTable.
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosAsync()
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

                string query = "SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario FROM Producto ORDER BY Id";

                using var cmd = new SqlCommand(query, connection);
                using var adapter = new SqlDataAdapter(cmd);

                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, "No hay productos en la base de datos");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} productos");
            }
            catch (SqlException ex) when (ex.Number == -1)
            {
                return (false, new DataTable(), "Error de conexión: Timeout o instancia no encontrada");
            }
            catch (SqlException ex) when (ex.Number == 18456)
            {
                return (false, new DataTable(), "Error de autenticación: Credenciales incorrectas");
            }
            catch (SqlException ex)
            {
                return (false, new DataTable(), $"Error de SQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene productos filtrados por categoría.
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorCategoriaAsync(string categoria)
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
                SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario 
                FROM Producto 
                WHERE Categoria LIKE @Categoria 
                ORDER BY Id";

                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@Categoria", $"%{categoria}%");

                using var adapter = new SqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                return ValidarYRetornarResultado(dataTable, $"No hay productos en la categoría '{categoria}'", 
                    $"Se obtuvieron {dataTable.Rows.Count} productos de la categoría '{categoria}'");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene productos dentro de un rango de precios.
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorPrecioAsync(decimal precioMin, decimal precioMax)
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
                SELECT Id, Nombre, Categoria, Valor, Cantidad, PrecioUnitario 
                FROM Producto 
                WHERE PrecioUnitario BETWEEN @PrecioMin AND @PrecioMax 
                ORDER BY PrecioUnitario";

                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@PrecioMin", precioMin);
                cmd.Parameters.AddWithValue("@PrecioMax", precioMax);

                using var adapter = new SqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                return ValidarYRetornarResultado(dataTable, $"No hay productos entre ${precioMin:F2} y ${precioMax:F2}", 
                    $"Se obtuvieron {dataTable.Rows.Count} productos");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene estadísticas de los productos.
        /// </summary>
        public async Task<(bool Success, Dictionary<string, object> Stats, string Message)> ObtenerEstadisticasAsync()
        {
            var stats = new Dictionary<string, object>();

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
                    COUNT(DISTINCT Categoria) as CategoríasUnicas,
                    MIN(PrecioUnitario) as PrecioMínimo,
                    MAX(PrecioUnitario) as PrecioMáximo,
                    AVG(PrecioUnitario) as PrecioPromedio,
                    SUM(Cantidad) as CantidadTotal,
                    SUM(Valor) as ValorTotal,
                    AVG(Cantidad) as CantidadPromedio
                FROM Producto";

                using var cmd = new SqlCommand(query, connection);
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    stats = new Dictionary<string, object>
                    {
                        { "Total", reader["Total"] },
                        { "CategoríasUnicas", reader["CategoríasUnicas"] },
                        { "PrecioMínimo", Convert.ToDecimal(reader["PrecioMínimo"]) },
                        { "PrecioMáximo", Convert.ToDecimal(reader["PrecioMáximo"]) },
                        { "PrecioPromedio", Convert.ToDecimal(reader["PrecioPromedio"]) },
                        { "CantidadTotal", reader["CantidadTotal"] },
                        { "ValorTotal", Convert.ToDecimal(reader["ValorTotal"]) },
                        { "CantidadPromedio", Convert.ToDecimal(reader["CantidadPromedio"]) }
                    };

                    return (true, stats, "Estadísticas obtenidas correctamente");
                }
            }
            catch (Exception ex)
            {
                return (false, stats, $"Error: {ex.Message}");
            }

            return (false, stats, "No se pudieron obtener las estadísticas");
        }

        /// <summary>
        /// Muestra un DataTable en la consola con paginación.
        /// </summary>
        public void MostrarTablaEnConsola(DataTable dataTable, string titulo = "DATOS DE SQL SERVER")
        {
            if (dataTable == null || dataTable.Rows.Count == 0)
            {
                Console.WriteLine("\n? No hay datos para mostrar.");
                return;
            }

            int anchoColumna = CalcularAnchoColumna(dataTable);
            int filasPorPagina = 10;
            int pagina = 0;
            int totalPaginas = (int)Math.Ceiling((double)dataTable.Rows.Count / filasPorPagina);

            while (true)
            {
                Console.Clear();
                MostrarCabeceraPaginada(titulo, pagina, totalPaginas, dataTable.Rows.Count, dataTable.Columns.Count);

                int inicio = pagina * filasPorPagina;
                int fin = Math.Min(inicio + filasPorPagina, dataTable.Rows.Count);

                MostrarTablaPaginada(dataTable, inicio, fin, anchoColumna);

                if (!ProcesarNavegacionPaginada(ref pagina, totalPaginas))
                    return;
            }
        }

        /// <summary>
        /// Valida y retorna resultado en base a filas encontradas.
        /// </summary>
        private (bool Success, DataTable Data, string Message) ValidarYRetornarResultado(DataTable dataTable, string mensajeVacio, string mensajeExito)
        {
            return dataTable.Rows.Count == 0
                ? (false, dataTable, mensajeVacio)
                : (true, dataTable, mensajeExito);
        }

        /// <summary>
        /// Muestra la cabecera paginada con información.
        /// </summary>
        private void MostrarCabeceraPaginada(string titulo, int pagina, int totalPaginas, int totalFilas, int totalColumnas)
        {
            Console.WriteLine("????????????????????????????????????????????????????????????????");
            Console.WriteLine($"?  {titulo}");
            Console.WriteLine($"?  PÁGINA {pagina + 1} de {totalPaginas} | Filas: {totalFilas} | Columnas: {totalColumnas}");
            Console.WriteLine("????????????????????????????????????????????????????????????????\n");
        }

        /// <summary>
        /// Procesa la navegación paginada.
        /// </summary>
        private bool ProcesarNavegacionPaginada(ref int pagina, int totalPaginas)
        {
            Console.WriteLine();
            Console.WriteLine("??????????????????????????????????????????");
            Console.WriteLine("?           OPCIONES DE NAVEGACIÓN       ?");
            Console.WriteLine("??????????????????????????????????????????");
            Console.WriteLine("?  [A] - Página anterior                 ?");
            Console.WriteLine("?  [S] - Página siguiente                ?");
            Console.WriteLine("?  [V] - Volver al menú principal        ?");
            Console.WriteLine("??????????????????????????????????????????");

            if (pagina == 0)
                Console.WriteLine("  (No hay página anterior)");

            if (pagina >= totalPaginas - 1)
                Console.WriteLine("  (No hay página siguiente)");

            Console.Write("\nIngrese una opción [A/S/V]: ");

            string opcion = (Console.ReadLine()?.ToUpper() ?? "").Trim();

            switch (opcion)
            {
                case "A" when pagina > 0:
                    pagina--;
                    return true;
                case "A":
                    MostrarMensaje("? No hay página anterior. Presione cualquier tecla...");
                    return true;
                case "S" when pagina < totalPaginas - 1:
                    pagina++;
                    return true;
                case "S":
                    MostrarMensaje("? No hay página siguiente. Presione cualquier tecla...");
                    return true;
                case "V":
                    return false;
                default:
                    MostrarMensaje("? Opción no válida. Presione cualquier tecla...");
                    return true;
            }
        }

        /// <summary>
        /// Muestra un mensaje en la consola y espera una tecla.
        /// </summary>
        private void MostrarMensaje(string mensaje)
        {
            Console.WriteLine($"\n{mensaje}");
            Console.ReadKey();
        }

        /// <summary>
        /// Calcula el ancho óptimo de columna.
        /// </summary>
        private int CalcularAnchoColumna(DataTable dataTable)
        {
            int ventanaAncho = Console.WindowWidth - 4;
            int numColumnas = dataTable.Columns.Count;
            int anchoDisponible = ventanaAncho / numColumnas;
            return Math.Min(anchoDisponible, 20);
        }

        /// <summary>
        /// Muestra una página de la tabla usando foreach y LINQ.
        /// </summary>
        private void MostrarTablaPaginada(DataTable dataTable, int filaInicio, int filaFin, int anchoColumna)
        {
            // Mostrar encabezados
            foreach (DataColumn column in dataTable.Columns)
            {
                string nombre = column.ColumnName.Length > anchoColumna
                    ? column.ColumnName.Substring(0, anchoColumna - 2) + ".."
                    : column.ColumnName;
                Console.Write(nombre.PadRight(anchoColumna) + "| ");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', Console.WindowWidth - 1));

            // Mostrar datos con LINQ (Skip, Take) + foreach
            foreach (var row in dataTable.Rows.Cast<DataRow>().Skip(filaInicio).Take(filaFin - filaInicio))
            {
                foreach (var cell in row.ItemArray.Select(cell => FormatearCelda(cell.ToString(), anchoColumna)))
                {
                    Console.Write(cell.PadRight(anchoColumna) + "| ");
                }
                Console.WriteLine();
            }
        }

        /// <summary>
        /// Formatea una celda de tabla con truncamiento si es necesario.
        /// </summary>
        private string FormatearCelda(string valor, int anchoColumna)
        {
            return valor.Length > anchoColumna
                ? valor.Substring(0, anchoColumna - 2) + ".."
                : valor;
        }
    }
}