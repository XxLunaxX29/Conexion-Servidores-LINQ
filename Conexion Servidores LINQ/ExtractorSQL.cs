using Microsoft.Data.SqlClient;
using System.Data;
using System.Net;
using System.Text;
using System.Xml;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para extraer datos de SQL Server y mostrarlos en tabla en consola.
    /// </summary>
    public class ExtractorSQL(string connectionString)
    {
        private const string DATABASE_NAME = "ConexionSQL";
        private const string TABLE_NAME = "DatosImportados";

        static ExtractorSQL()
        {
            // Ignorar validación de certificado SSL (solo para desarrollo)
            ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        }

        /// <summary>
        /// Obtiene todos los datos desde SQL Server como DataTable.
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosAsync()
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

                string query = $"SELECT * FROM [{TABLE_NAME}]";

                using var cmd = new SqlCommand(query, connection);
                using var adapter = new SqlDataAdapter(cmd);

                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay datos en la tabla '{TABLE_NAME}'");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros");
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
        /// Obtiene datos filtrados por categoría (si existe la columna).
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorCategoriaAsync(string categoria)
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

                // Verificar si la columna existe
                string checkColumn = $@"
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{TABLE_NAME}' AND COLUMN_NAME = 'Categoria'";

                using var checkCmd = new SqlCommand(checkColumn, connection);
                int columnExists = (int)await checkCmd.ExecuteScalarAsync();

                if (columnExists == 0)
                {
                    return (false, new DataTable(), "La columna 'Categoria' no existe en la tabla");
                }

                string query = $@"
                    SELECT * FROM [{TABLE_NAME}] 
                    WHERE Categoria LIKE @Categoria";

                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@Categoria", $"%{categoria}%");

                using var adapter = new SqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay datos con categoría '{categoria}'");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros de la categoría '{categoria}'");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene datos dentro de un rango de precios (si existe la columna).
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorPrecioAsync(decimal precioMin, decimal precioMax)
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

                // Detectar columna de precio (puede ser PrecioUnitario, Precio, Valor, etc.)
                string detectColumn = $@"
                    SELECT TOP 1 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{TABLE_NAME}' 
                    AND (COLUMN_NAME LIKE '%precio%' OR COLUMN_NAME LIKE '%valor%')
                    ORDER BY ORDINAL_POSITION";

                using var detectCmd = new SqlCommand(detectColumn, connection);
                var columnaPrecio = await detectCmd.ExecuteScalarAsync() as string;

                if (columnaPrecio == null)
                {
                    return (false, new DataTable(), "No hay columna de precio en la tabla");
                }

                string query = $@"
                    SELECT * FROM [{TABLE_NAME}] 
                    WHERE [{columnaPrecio}] BETWEEN @PrecioMin AND @PrecioMax";

                using var cmd = new SqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@PrecioMin", precioMin);
                cmd.Parameters.AddWithValue("@PrecioMax", precioMax);

                using var adapter = new SqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay datos entre ${precioMin:F2} y ${precioMax:F2}");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene estadísticas de los datos.
        /// </summary>
        public async Task<(bool Success, Dictionary<string, object> Stats, string Message)> ObtenerEstadisticasAsync()
        {
            var stats = new Dictionary<string, object>();

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
                var total = await cmd.ExecuteScalarAsync();

                stats["Total"] = total ?? 0;
                stats["CategoriasUnicas"] = 0;
                stats["PrecioMinimo"] = 0;
                stats["PrecioMaximo"] = 0;
                stats["PrecioPromedio"] = 0;
                stats["CantidadTotal"] = 0;
                stats["ValorTotal"] = 0;
                stats["CantidadPromedio"] = 0;

                return (true, stats, "Estadísticas obtenidas correctamente");
            }
            catch (Exception ex)
            {
                return (false, stats, $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Exporta DataTable a CSV.
        /// </summary>
        public bool ExportarACSV(DataTable dataTable, string rutaArchivo)
        {
            try
            {
                if (dataTable == null || dataTable.Rows.Count == 0)
                {
                    Console.WriteLine("? El DataTable está vacío.");
                    return false;
                }

                using var writer = new StreamWriter(rutaArchivo, false, Encoding.UTF8);

                // Escribir encabezados
                var encabezados = dataTable.Columns.Cast<DataColumn>().Select(c => EscaparCSV(c.ColumnName));
                writer.WriteLine(string.Join(",", encabezados));

                // Escribir datos
                foreach (DataRow row in dataTable.Rows)
                {
                    var valores = row.ItemArray.Select(v => EscaparCSV(v?.ToString() ?? ""));
                    writer.WriteLine(string.Join(",", valores));
                }

                Console.WriteLine($"? Archivo CSV exportado correctamente en: {rutaArchivo}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al exportar CSV: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Exporta DataTable a JSON.
        /// </summary>
        public bool ExportarAJSON(DataTable dataTable, string rutaArchivo)
        {
            try
            {
                if (dataTable == null || dataTable.Rows.Count == 0)
                {
                    Console.WriteLine("? El DataTable está vacío.");
                    return false;
                }

                var jsonArray = new System.Collections.Generic.List<Dictionary<string, object>>();

                foreach (DataRow row in dataTable.Rows)
                {
                    var jsonObj = new Dictionary<string, object>();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        jsonObj[col.ColumnName] = row[col] ?? "";
                    }
                    jsonArray.Add(jsonObj);
                }

                var json = System.Text.Json.JsonSerializer.Serialize(jsonArray, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });

                File.WriteAllText(rutaArchivo, json, Encoding.UTF8);
                Console.WriteLine($"? Archivo JSON exportado correctamente en: {rutaArchivo}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al exportar JSON: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Exporta DataTable a XML.
        /// </summary>
        public bool ExportarAXML(DataTable dataTable, string rutaArchivo)
        {
            try
            {
                if (dataTable == null || dataTable.Rows.Count == 0)
                {
                    Console.WriteLine("? El DataTable está vacío.");
                    return false;
                }

                var xmlDoc = new XmlDocument();
                var rootElement = xmlDoc.CreateElement("datos");
                xmlDoc.AppendChild(rootElement);

                foreach (DataRow row in dataTable.Rows)
                {
                    var rowElement = xmlDoc.CreateElement("fila");
                    rootElement.AppendChild(rowElement);

                    foreach (DataColumn col in dataTable.Columns)
                    {
                        var colElement = xmlDoc.CreateElement(col.ColumnName);
                        colElement.InnerText = row[col]?.ToString() ?? "";
                        rowElement.AppendChild(colElement);
                    }
                }

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    Encoding = Encoding.UTF8
                };

                using var writer = XmlWriter.Create(rutaArchivo, settings);
                xmlDoc.WriteTo(writer);

                Console.WriteLine($"? Archivo XML exportado correctamente en: {rutaArchivo}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al exportar XML: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Escapa caracteres especiales para CSV.
        /// </summary>
        private string EscaparCSV(string valor)
        {
            if (string.IsNullOrEmpty(valor))
                return "\"\"";

            if (valor.Contains(",") || valor.Contains("\"") || valor.Contains("\n"))
            {
                return $"\"{valor.Replace("\"", "\"\"")}\"";
            }

            return valor;
        }

        /// <summary>
        /// Muestra un DataTable en la consola con paginación y opción de exportar.
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
                Console.WriteLine("??????????????????????????????????????????????????????????????????");
                Console.WriteLine($"?  {titulo}");
                Console.WriteLine($"?  PÁGINA {pagina + 1} de {totalPaginas} | Filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}");
                Console.WriteLine("??????????????????????????????????????????????????????????????????\n");

                int inicio = pagina * filasPorPagina;
                int fin = Math.Min(inicio + filasPorPagina, dataTable.Rows.Count);

                MostrarTablaPaginada(dataTable, inicio, fin, anchoColumna);

                Console.WriteLine();
                Console.WriteLine("????????????????????????????????????????");
                Console.WriteLine("?      OPCIONES DE NAVEGACIÓN         ?");
                Console.WriteLine("????????????????????????????????????????");
                Console.WriteLine("?  [A] - Página anterior               ?");
                Console.WriteLine("?  [S] - Página siguiente              ?");
                Console.WriteLine("?  [E] - Exportar datos                ?");
                Console.WriteLine("?  [V] - Volver al menú principal      ?");
                Console.WriteLine("????????????????????????????????????????");

                if (pagina == 0)
                    Console.WriteLine("  (No hay página anterior)");

                if (pagina >= totalPaginas - 1)
                    Console.WriteLine("  (No hay página siguiente)");

                Console.Write("\nIngrese una opción [A/S/E/V]: ");

                string nav = Console.ReadLine()?.ToUpper() ?? "";

                switch (nav)
                {
                    case "A":
                        if (pagina > 0)
                            pagina--;
                        else
                        {
                            Console.WriteLine("\n? No hay página anterior. Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        break;

                    case "S":
                        if (pagina < totalPaginas - 1)
                            pagina++;
                        else
                        {
                            Console.WriteLine("\n? No hay página siguiente. Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        break;

                    case "E":
                        ExportarDatos(dataTable);
                        break;

                    case "V":
                        return;

                    default:
                        Console.WriteLine("\n? Opción no válida. Presione cualquier tecla...");
                        Console.ReadKey();
                        break;
                }
            }
        }

        /// <summary>
        /// Muestra el menú de exportación.
        /// </summary>
        private void ExportarDatos(DataTable dataTable)
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("??????????????????????????????????????????");
                Console.WriteLine("?         EXPORTAR DATOS                 ?");
                Console.WriteLine("??????????????????????????????????????????\n");

                Console.WriteLine("Seleccione el formato de exportación:");
                Console.WriteLine("1. CSV");
                Console.WriteLine("2. JSON");
                Console.WriteLine("3. XML");
                Console.WriteLine("4. Volver");
                Console.Write("\nOpción: ");

                string opcion = Console.ReadLine();

                string nombreArchivo = $"exportacion_{DateTime.Now:yyyyMMdd_HHmmss}";
                string rutaBase = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);

                switch (opcion)
                {
                    case "1":
                        string rutaCSV = Path.Combine(rutaBase, $"{nombreArchivo}.csv");
                        ExportarACSV(dataTable, rutaCSV);
                        Console.WriteLine("Presione cualquier tecla...");
                        Console.ReadKey();
                        return;

                    case "2":
                        string rutaJSON = Path.Combine(rutaBase, $"{nombreArchivo}.json");
                        ExportarAJSON(dataTable, rutaJSON);
                        Console.WriteLine("Presione cualquier tecla...");
                        Console.ReadKey();
                        return;

                    case "3":
                        string rutaXML = Path.Combine(rutaBase, $"{nombreArchivo}.xml");
                        ExportarAXML(dataTable, rutaXML);
                        Console.WriteLine("Presione cualquier tecla...");
                        Console.ReadKey();
                        return;

                    case "4":
                        return;

                    default:
                        Console.WriteLine("\n? Opción no válida. Presione cualquier tecla...");
                        Console.ReadKey();
                        break;
                }
            }
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
        /// Muestra una página de la tabla.
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

            // Mostrar datos
            for (int i = filaInicio; i < filaFin; i++)
            {
                foreach (var cell in dataTable.Rows[i].ItemArray)
                {
                    string valor = cell.ToString();
                    if (valor.Length > anchoColumna)
                        valor = valor.Substring(0, anchoColumna - 2) + "..";
                    Console.Write(valor.PadRight(anchoColumna) + "| ");
                }
                Console.WriteLine();
            }
        }
    }
}