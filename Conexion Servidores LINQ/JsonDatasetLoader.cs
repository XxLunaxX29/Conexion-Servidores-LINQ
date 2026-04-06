using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que carga archivos JSON y los estructura en una tabla evitando duplicados y columnas vacías.
    /// </summary>
    public class JsonDatasetLoader
    {
        private DataTable _dataTable;

        public JsonDatasetLoader()
        {
            _dataTable = new DataTable();
        }

        /// <summary>
        /// Carga un archivo JSON y lo convierte en una tabla de datos limpia.
        /// </summary>
        /// <param name="jsonFilePath">Ruta del archivo JSON</param>
        /// <returns>DataTable con los datos cargados</returns>
        public DataTable LoadJsonFile(string jsonFilePath)
        {
            try
            {
                if (!File.Exists(jsonFilePath))
                    throw new FileNotFoundException($"El archivo JSON no existe: {jsonFilePath}");

                Console.WriteLine("\n? Leyendo archivo JSON...");
                _dataTable = new DataTable();

                string jsonContent = File.ReadAllText(jsonFilePath);

                if (string.IsNullOrWhiteSpace(jsonContent))
                    throw new InvalidOperationException("El archivo JSON está vacío.");

                Console.WriteLine("? Parseando contenido JSON...");

                using (JsonDocument doc = JsonDocument.Parse(jsonContent))
                {
                    JsonElement root = doc.RootElement;
                    
                    // LINQ: Detectar si es un array o un objeto único
                    var elementos = root.ValueKind == JsonValueKind.Array
                        ? root.EnumerateArray().ToList()
                        : ExtraerArrayDelObjeto(root);

                    if (elementos.Count == 0)
                        throw new InvalidOperationException("No se encontraron elementos en el JSON.");

                    Console.WriteLine($"? Se encontraron {elementos.Count} elementos");

                    // LINQ: Extraer todas las claves (columnas) ordenadas
                    Console.WriteLine("? Extrayendo columnas...");
                    var todasLasClaves = ExtraerTodasLasClaves(elementos);

                    // Crear columnas en la tabla
                    foreach (var clave in todasLasClaves)
                    {
                        _dataTable.Columns.Add(clave, typeof(string));
                    }

                    Console.WriteLine($"? Se crearon {todasLasClaves.Count} columnas");

                    // Agregar filas evitando duplicados
                    var filasAgregadas = new HashSet<string>();
                    int filasEnProceso = 0;
                    int filasValidas = 0;

                    Console.WriteLine("? Procesando filas de datos...\n");

                    foreach (var elemento in elementos)
                    {
                        if (elemento.ValueKind != JsonValueKind.Object)
                            continue;

                        filasEnProceso++;
                        
                        // LINQ: Crear datos de fila
                        var rowData = todasLasClaves
                            .Select(clave => ObtenerValor(elemento, clave))
                            .ToArray();
                        
                        var rowHash = string.Join("|", rowData);

                        // Evitar duplicados
                        if (!filasAgregadas.Contains(rowHash))
                        {
                            _dataTable.Rows.Add(rowData);
                            filasAgregadas.Add(rowHash);
                            filasValidas++;
                        }

                        if (filasEnProceso % 10 == 0)
                        {
                            Console.Write($"\r? Procesadas {filasEnProceso} filas | Agregadas {filasValidas} filas");
                        }
                    }

                    // Eliminar columnas completamente vacías
                    RemoverColumnasVacias();

                    Console.WriteLine($"\r? Carga completada: {filasEnProceso} leídas | {filasValidas} agregadas (sin duplicados)");
                }

                return _dataTable;
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Error al parsear JSON: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al cargar el archivo JSON: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Extrae array del objeto root usando LINQ.
        /// </summary>
        private List<JsonElement> ExtraerArrayDelObjeto(JsonElement root)
        {
            var arrayProperty = root.EnumerateObject()
                .FirstOrDefault(p => p.Value.ValueKind == JsonValueKind.Array);

            return arrayProperty.Value.ValueKind == JsonValueKind.Array
                ? arrayProperty.Value.EnumerateArray().ToList()
                : new List<JsonElement>();
        }

        /// <summary>
        /// Extrae todas las claves únicas del conjunto de elementos JSON usando LINQ.
        /// </summary>
        private List<string> ExtraerTodasLasClaves(List<JsonElement> elementos)
        {
            return elementos
                .Where(e => e.ValueKind == JsonValueKind.Object)
                .SelectMany(e => e.EnumerateObject().Select(p => p.Name))
                .Distinct()
                .OrderBy(c => c)
                .ToList();
        }

        /// <summary>
        /// Obtiene el valor de una propiedad específica del elemento JSON.
        /// </summary>
        private string ObtenerValor(JsonElement elemento, string nombrePropiedad)
        {
            return elemento.TryGetProperty(nombrePropiedad, out JsonElement propiedad)
                ? ConvertirValorJson(propiedad)
                : string.Empty;
        }

        /// <summary>
        /// Convierte un valor JSON a string, manejando diferentes tipos de datos.
        /// </summary>
        private string ConvertirValorJson(JsonElement elemento)
        {
            return elemento.ValueKind switch
            {
                JsonValueKind.String => elemento.GetString() ?? string.Empty,
                
                JsonValueKind.Number => elemento.TryGetInt32(out int intValue)
                    ? intValue.ToString()
                    : elemento.TryGetInt64(out long longValue)
                    ? longValue.ToString()
                    : elemento.TryGetDouble(out double doubleValue)
                    ? doubleValue.ToString()
                    : elemento.ToString(),
                
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                JsonValueKind.Null => string.Empty,
                
                JsonValueKind.Array => string.Join("; ", elemento.EnumerateArray()
                    .Select(e => ConvertirValorJson(e))
                    .Where(s => !string.IsNullOrEmpty(s))),
                
                JsonValueKind.Object => "{" + string.Join(", ", elemento.EnumerateObject()
                    .Select(p => $"{p.Name}: {ConvertirValorJson(p.Value)}")) + "}",
                
                _ => string.Empty
            };
        }

        /// <summary>
        /// Elimina columnas que están completamente vacías usando LINQ.
        /// </summary>
        private void RemoverColumnasVacias()
        {
            var columnasVacias = _dataTable.Columns.Cast<DataColumn>()
                .Where(column => _dataTable.AsEnumerable()
                    .All(row => string.IsNullOrWhiteSpace(row[column].ToString())))
                .ToList();

            foreach (var column in columnasVacias)
            {
                _dataTable.Columns.Remove(column);
            }
        }

        /// <summary>
        /// Obtiene la tabla de datos cargada.
        /// </summary>
        public DataTable GetDataTable()
        {
            return _dataTable;
        }

        /// <summary>
        /// Exporta la tabla a un archivo CSV usando LINQ.
        /// </summary>
        /// <param name="csvFilePath">Ruta del archivo CSV a guardar</param>
        public void ExportToCsv(string csvFilePath)
        {
            try
            {
                using (var writer = new StreamWriter(csvFilePath, false, System.Text.Encoding.UTF8))
                {
                    // LINQ: Escribir encabezados
                    var headers = string.Join(",", _dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
                    writer.WriteLine(headers);

                    // LINQ: Escribir datos
                    foreach (DataRow row in _dataTable.Rows)
                    {
                        writer.WriteLine(string.Join(",", row.ItemArray.Select(v => $"\"{v}\"")));
                    }
                }

                Console.WriteLine($"Tabla exportada correctamente a: {csvFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al exportar a CSV: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Muestra la tabla en la consola de forma formateada usando LINQ.
        /// </summary>
        public void DisplayTable()
        {
            if (_dataTable.Rows.Count == 0)
            {
                Console.WriteLine("La tabla está vacía.");
                return;
            }

            Console.WriteLine("\n" + new string('=', 80));
            Console.WriteLine($"Total de filas: {_dataTable.Rows.Count}");
            Console.WriteLine(new string('=', 80));

            // LINQ: Mostrar encabezados
            var headers = string.Join("| ", _dataTable.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName.PadRight(20)));
            Console.WriteLine(headers);
            Console.WriteLine(new string('-', 80));

            // LINQ: Mostrar datos
            foreach (DataRow row in _dataTable.Rows)
            {
                Console.WriteLine(string.Join("| ", row.ItemArray
                    .Select(cell =>
                    {
                        string cellValue = cell.ToString();
                        return cellValue.Length > 20 ? cellValue.Substring(0, 17) + "..." : cellValue.PadRight(20);
                    })));
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}