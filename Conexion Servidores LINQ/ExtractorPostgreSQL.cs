using Npgsql;
using System.Data;
using System.Text.Json;
using System.Xml;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase para extraer datos de PostgreSQL y mostrarlos en tabla en consola.
    /// Funciona con la tabla DatosImportados creada por las migraciones.
    /// </summary>
    public class ExtractorPostgreSQL(string connectionString)
    {
        private const string DATABASE_NAME = "ConexionPostgreSQL";
        private const string TABLE_NAME = "DatosImportados";

        /// <summary>
        /// Obtiene todos los registros desde PostgreSQL como DataTable.
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosAsync()
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $"SELECT * FROM \"{TABLE_NAME}\" LIMIT 10000";

                using var cmd = new NpgsqlCommand(query, connection);
                using var adapter = new NpgsqlDataAdapter(cmd);

                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay datos en la tabla '{TABLE_NAME}'");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros");
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("could not connect"))
            {
                return (false, new DataTable(), "Error de conexión: No se pudo conectar a PostgreSQL");
            }
            catch (NpgsqlException ex) when (ex.Message.Contains("password"))
            {
                return (false, new DataTable(), "Error de autenticación: Credenciales incorrectas");
            }
            catch (NpgsqlException ex)
            {
                return (false, new DataTable(), $"Error de PostgreSQL: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene registros filtrados por categoría (si existe la columna).
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorCategoriaAsync(string categoria)
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Verificar si la columna Categoria existe
                string checkColumn = $@"
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = '{TABLE_NAME}' 
                    AND column_name = 'Categoria'";

                using var checkCmd = new NpgsqlCommand(checkColumn, connection);
                var columnExists = await checkCmd.ExecuteScalarAsync();

                if (columnExists == null || (long)columnExists == 0)
                {
                    return (false, new DataTable(), $"La columna 'Categoria' no existe en la tabla '{TABLE_NAME}'");
                }

                string query = $@"
                    SELECT * FROM ""{TABLE_NAME}"" 
                    WHERE ""Categoria"" ILIKE @Categoria 
                    LIMIT 10000";

                using var cmd = new NpgsqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@Categoria", $"%{categoria}%");

                using var adapter = new NpgsqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay registros con categoría '{categoria}'");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros de la categoría '{categoria}'");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene registros dentro de un rango de precios (si existe columna numérica).
        /// </summary>
        public async Task<(bool Success, DataTable Data, string Message)> ObtenerProductosPorPrecioAsync(decimal precioMin, decimal precioMax)
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Detectar columna numérica tipo precio o valor
                string detectColumn = $@"
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = '{TABLE_NAME}' 
                    AND udt_name IN ('numeric', 'double precision', 'real')
                    LIMIT 1";

                using var detectCmd = new NpgsqlCommand(detectColumn, connection);
                var columnaPrecio = await detectCmd.ExecuteScalarAsync() as string;

                if (columnaPrecio == null)
                {
                    return (false, new DataTable(), "No hay columna numérica para filtrar por precio");
                }

                string query = $@"
                    SELECT * FROM ""{TABLE_NAME}"" 
                    WHERE ""{columnaPrecio}"" BETWEEN @PrecioMin AND @PrecioMax 
                    LIMIT 10000";

                using var cmd = new NpgsqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@PrecioMin", precioMin);
                cmd.Parameters.AddWithValue("@PrecioMax", precioMax);

                using var adapter = new NpgsqlDataAdapter(cmd);
                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count == 0)
                {
                    return (false, dataTable, $"No hay registros entre ${precioMin:F2} y ${precioMax:F2}");
                }

                return (true, dataTable, $"Se obtuvieron {dataTable.Rows.Count} registros");
            }
            catch (Exception ex)
            {
                return (false, new DataTable(), $"Error: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtiene estadísticas básicas de la tabla.
        /// </summary>
        public async Task<(bool Success, Dictionary<string, object> Stats, string Message)> ObtenerEstadisticasAsync()
        {
            var stats = new Dictionary<string, object>();

            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DATABASE_NAME
                };

                using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                string query = $@"
                SELECT 
                    COUNT(*) as Total
                FROM ""{TABLE_NAME}""";

                using var cmd = new NpgsqlCommand(query, connection);
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    stats["Total"] = reader["Total"];

                    // Obtener info de columnas
                    string columnQuery = $@"
                        SELECT COUNT(*) as TotalColumnas 
                        FROM information_schema.columns 
                        WHERE table_name = '{TABLE_NAME}' 
                        AND table_schema = 'public'";

                    using var colCmd = new NpgsqlCommand(columnQuery, connection);
                    var totalCol = await colCmd.ExecuteScalarAsync();
                    stats["TotalColumnas"] = totalCol ?? 0;

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

                using var writer = new StreamWriter(rutaArchivo, false, System.Text.Encoding.UTF8);

                // Escribir encabezados
                var encabezados = new List<string>();
                foreach (DataColumn col in dataTable.Columns)
                {
                    encabezados.Add(col.ColumnName);
                }
                writer.WriteLine(string.Join(",", encabezados));

                // Escribir datos
                foreach (DataRow row in dataTable.Rows)
                {
                    var valores = new List<string>();
                    foreach (var cell in row.ItemArray)
                    {
                        string valor = cell?.ToString() ?? "";
                        // Escapar comillas y envolver en comillas si contiene comas
                        if (valor.Contains(",") || valor.Contains("\"") || valor.Contains("\n"))
                        {
                            valor = $"\"{valor.Replace("\"", "\"\"")}\"";
                        }
                        valores.Add(valor);
                    }
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

                var jsonArray = new List<Dictionary<string, object>>();

                foreach (DataRow row in dataTable.Rows)
                {
                    var jsonObj = new Dictionary<string, object>();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        jsonObj[col.ColumnName] = row[col] ?? "";
                    }
                    jsonArray.Add(jsonObj);
                }

                var json = JsonSerializer.Serialize(jsonArray, new JsonSerializerOptions
                {
                    WriteIndented = true,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                });

                File.WriteAllText(rutaArchivo, json, System.Text.Encoding.UTF8);
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
                rootElement.SetAttribute("fecha", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                rootElement.SetAttribute("totalRegistros", dataTable.Rows.Count.ToString());
                xmlDoc.AppendChild(rootElement);

                foreach (DataRow row in dataTable.Rows)
                {
                    var rowElement = xmlDoc.CreateElement("registro");
                    rootElement.AppendChild(rowElement);

                    foreach (DataColumn col in dataTable.Columns)
                    {
                        var colElement = xmlDoc.CreateElement(SanitizarNombreXML(col.ColumnName));
                        colElement.InnerText = row[col]?.ToString() ?? "";
                        rowElement.AppendChild(colElement);
                    }
                }

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    Encoding = System.Text.Encoding.UTF8
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
        /// Sanitiza nombres de columnas para que sean válidos en XML.
        /// </summary>
        private string SanitizarNombreXML(string nombre)
        {
            if (string.IsNullOrEmpty(nombre))
                return "campo";

            // Reemplazar espacios y caracteres especiales
            var sanitizado = System.Text.RegularExpressions.Regex.Replace(nombre, @"[^a-zA-Z0-9_-]", "_");

            // Si comienza con número, agregar prefijo
            if (char.IsDigit(sanitizado[0]))
                sanitizado = "_" + sanitizado;

            return sanitizado;
        }

        /// <summary>
        /// Muestra un DataTable en la consola con paginación y opción de exportar.
        /// </summary>
        public void MostrarTablaEnConsola(DataTable dataTable, string titulo = "DATOS DE POSTGRESQL")
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
                Console.WriteLine($"?  {titulo.PadRight(62)} ?");
                Console.WriteLine($"?  PÁGINA {pagina + 1} de {totalPaginas} | Filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}".PadRight(63) + "?");
                Console.WriteLine("??????????????????????????????????????????????????????????????????\n");

                int inicio = pagina * filasPorPagina;
                int fin = Math.Min(inicio + filasPorPagina, dataTable.Rows.Count);

                MostrarTablaPaginada(dataTable, inicio, fin, anchoColumna);

                Console.WriteLine();
                Console.WriteLine("????????????????????????????????????????");
                Console.WriteLine("?      OPCIONES DE NAVEGACIÓN          ?");
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
                        MostrarMenuExportacion(dataTable);
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
        /// Muestra el menú de exportación de datos.
        /// </summary>
        private void MostrarMenuExportacion(DataTable dataTable)
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("??????????????????????????????????????????");
                Console.WriteLine("?       EXPORTAR DATOS                   ?");
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
                        if (ExportarACSV(dataTable, rutaCSV))
                        {
                            Console.WriteLine("? Archivo guardado en Escritorio");
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        else
                        {
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        return;

                    case "2":
                        string rutaJSON = Path.Combine(rutaBase, $"{nombreArchivo}.json");
                        if (ExportarAJSON(dataTable, rutaJSON))
                        {
                            Console.WriteLine("? Archivo guardado en Escritorio");
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        else
                        {
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        return;

                    case "3":
                        string rutaXML = Path.Combine(rutaBase, $"{nombreArchivo}.xml");
                        if (ExportarAXML(dataTable, rutaXML))
                        {
                            Console.WriteLine("? Archivo guardado en Escritorio");
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
                        else
                        {
                            Console.WriteLine("Presione cualquier tecla...");
                            Console.ReadKey();
                        }
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
            foreach (DataColumn column in dataTable.Columns)
            {
                string nombre = column.ColumnName.Length > anchoColumna
                    ? column.ColumnName.Substring(0, anchoColumna - 2) + ".."
                    : column.ColumnName;
                Console.Write(nombre.PadRight(anchoColumna) + "| ");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', Console.WindowWidth - 1));

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