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

                // Parsear el JSON
                using (JsonDocument doc = JsonDocument.Parse(jsonContent))
                {
                    JsonElement root = doc.RootElement;
                    List<JsonElement> elementos = new List<JsonElement>();

                    // Detectar si es un array o un objeto único
                    if (root.ValueKind == JsonValueKind.Array)
                    {
                        elementos = root.EnumerateArray().ToList();
                    }
                    else if (root.ValueKind == JsonValueKind.Object)
                    {
                        // Buscar arrays dentro del objeto
                        var arrayProperties = root.EnumerateObject()
                            .Where(p => p.Value.ValueKind == JsonValueKind.Array)
                            .ToList();

                        if (arrayProperties.Count > 0)
                        {
                            // Usar el primer array encontrado
                            elementos = arrayProperties[0].Value.EnumerateArray().ToList();
                        }
                        else
                        {
                            // Si no hay arrays, tratar el objeto como un único registro
                            elementos.Add(root);
                        }
                    }

                    if (elementos.Count == 0)
                        throw new InvalidOperationException("No se encontraron elementos en el JSON.");

                    Console.WriteLine($"? Se encontraron {elementos.Count} elementos");

                    // Extraer todas las claves (columnas)
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
                        var rowData = new List<string>();
                        var rowHash = "";

                        foreach (var clave in todasLasClaves)
                        {
                            string valor = ObtenerValor(elemento, clave);
                            rowData.Add(valor);
                            rowHash += valor + "|";
                        }

                        // Evitar duplicados
                        if (!filasAgregadas.Contains(rowHash))
                        {
                            _dataTable.Rows.Add(rowData.ToArray());
                            filasAgregadas.Add(rowHash);
                            filasValidas++;
                        }

                        // Mostrar progreso cada 10 filas
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
        /// Extrae todas las claves únicas del conjunto de elementos JSON.
        /// </summary>
        private List<string> ExtraerTodasLasClaves(List<JsonElement> elementos)
        {
            var claves = new HashSet<string>();

            foreach (var elemento in elementos)
            {
                if (elemento.ValueKind != JsonValueKind.Object)
                    continue;

                foreach (var propiedad in elemento.EnumerateObject())
                {
                    claves.Add(propiedad.Name);
                }
            }

            return claves.OrderBy(c => c).ToList();
        }

        /// <summary>
        /// Obtiene el valor de una propiedad específica del elemento JSON.
        /// </summary>
        private string ObtenerValor(JsonElement elemento, string nombrePropiedad)
        {
            try
            {
                if (elemento.TryGetProperty(nombrePropiedad, out JsonElement propiedad))
                {
                    return ConvertirValorJson(propiedad);
                }
                return string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Convierte un valor JSON a string, manejando diferentes tipos de datos.
        /// </summary>
        private string ConvertirValorJson(JsonElement elemento)
        {
            switch (elemento.ValueKind)
            {
                case JsonValueKind.String:
                    return elemento.GetString() ?? string.Empty;

                case JsonValueKind.Number:
                    if (elemento.TryGetInt32(out int intValue))
                        return intValue.ToString();
                    if (elemento.TryGetInt64(out long longValue))
                        return longValue.ToString();
                    if (elemento.TryGetDouble(out double doubleValue))
                        return doubleValue.ToString();
                    return elemento.ToString();

                case JsonValueKind.True:
                    return "true";

                case JsonValueKind.False:
                    return "false";

                case JsonValueKind.Null:
                    return string.Empty;

                case JsonValueKind.Array:
                    var items = elemento.EnumerateArray()
                        .Select(e => ConvertirValorJson(e))
                        .Where(s => !string.IsNullOrEmpty(s))
                        .ToList();
                    return string.Join("; ", items);

                case JsonValueKind.Object:
                    // Para objetos anidados, crear una representación simple
                    var propiedades = elemento.EnumerateObject()
                        .Select(p => $"{p.Name}: {ConvertirValorJson(p.Value)}")
                        .ToList();
                    return "{" + string.Join(", ", propiedades) + "}";

                default:
                    return string.Empty;
            }
        }

        /// <summary>
        /// Elimina columnas que están completamente vacías.
        /// </summary>
        private void RemoverColumnasVacias()
        {
            var columnasVacias = new List<DataColumn>();

            foreach (DataColumn column in _dataTable.Columns)
            {
                bool estaVacia = _dataTable.AsEnumerable()
                    .All(row => string.IsNullOrWhiteSpace(row[column].ToString()));

                if (estaVacia)
                    columnasVacias.Add(column);
            }

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
        /// Exporta la tabla a un archivo CSV.
        /// </summary>
        /// <param name="csvFilePath">Ruta del archivo CSV a guardar</param>
        public void ExportToCsv(string csvFilePath)
        {
            try
            {
                using (var writer = new StreamWriter(csvFilePath, false, System.Text.Encoding.UTF8))
                {
                    // Escribir encabezados
                    var headers = _dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName);
                    writer.WriteLine(string.Join(",", headers));

                    // Escribir datos
                    foreach (DataRow row in _dataTable.Rows)
                    {
                        var values = row.ItemArray.Select(v => $"\"{v}\"");
                        writer.WriteLine(string.Join(",", values));
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
        /// Muestra la tabla en la consola de forma formateada.
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

            // Mostrar encabezados
            foreach (DataColumn column in _dataTable.Columns)
            {
                Console.Write(column.ColumnName.PadRight(20) + "| ");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', 80));

            // Mostrar datos
            foreach (DataRow row in _dataTable.Rows)
            {
                foreach (var cell in row.ItemArray)
                {
                    Console.Write(cell.ToString().PadRight(20).Substring(0, 20) + "| ");
                }
                Console.WriteLine();
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}