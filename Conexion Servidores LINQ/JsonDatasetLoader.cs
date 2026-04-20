using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que carga archivos JSON y los estructura en una tabla evitando duplicados y columnas vacías.
    /// Utiliza Quick Sort y Bubble Sort para ordenamiento en lugar de LINQ.
    /// </summary>
    public class JsonDatasetLoader
    {
        private DataTable _dataTable;
        private bool _usarQuickSort = true; // true para Quick Sort, false para Bubble Sort

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
                        foreach (var elemento in root.EnumerateArray())
                        {
                            elementos.Add(elemento);
                        }
                    }
                    else if (root.ValueKind == JsonValueKind.Object)
                    {
                        // Buscar arrays dentro del objeto sin LINQ
                        var arrayProperties = new List<JsonProperty>();
                        foreach (var propiedad in root.EnumerateObject())
                        {
                            if (propiedad.Value.ValueKind == JsonValueKind.Array)
                            {
                                arrayProperties.Add(propiedad);
                            }
                        }

                        if (arrayProperties.Count > 0)
                        {
                            // Usar el primer array encontrado
                            foreach (var elemento in arrayProperties[0].Value.EnumerateArray())
                            {
                                elementos.Add(elemento);
                            }
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
                Console.WriteLine($"? Error al parsear JSON: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al cargar el archivo JSON: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Extrae todas las claves únicas del conjunto de elementos JSON.
        /// Ordena usando Quick Sort o Bubble Sort según el atributo _usarQuickSort.
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

            // Convertir HashSet a List
            var clavesList = new List<string>();
            foreach (var clave in claves)
            {
                clavesList.Add(clave);
            }

            // Ordenar usando Quick Sort o Bubble Sort
            if (_usarQuickSort)
            {
                OrdenarConQuickSort(clavesList, 0, clavesList.Count - 1);
            }
            else
            {
                OrdenarConBubbleSort(clavesList);
            }

            return clavesList;
        }

        /// <summary>
        /// Ordena una lista de strings usando Quick Sort (divide y conquista).
        /// </summary>
        private void OrdenarConQuickSort(List<string> items, int izquierda, int derecha)
        {
            if (izquierda < derecha)
            {
                int pivote = Particionar(items, izquierda, derecha);
                OrdenarConQuickSort(items, izquierda, pivote - 1);
                OrdenarConQuickSort(items, pivote + 1, derecha);
            }
        }

        /// <summary>
        /// Particiona la lista para Quick Sort usando el último elemento como pivote.
        /// </summary>
        private int Particionar(List<string> items, int izquierda, int derecha)
        {
            string pivote = items[derecha];
            int i = izquierda - 1;

            for (int j = izquierda; j < derecha; j++)
            {
                if (string.Compare(items[j], pivote) < 0)
                {
                    i++;
                    // Intercambiar
                    string temp = items[i];
                    items[i] = items[j];
                    items[j] = temp;
                }
            }

            // Intercambiar pivote a su posición correcta
            string tempPivote = items[i + 1];
            items[i + 1] = items[derecha];
            items[derecha] = tempPivote;

            return i + 1;
        }

        /// <summary>
        /// Ordena una lista de strings usando Bubble Sort (comparación simple).
        /// </summary>
        private void OrdenarConBubbleSort(List<string> items)
        {
            int n = items.Count;

            for (int i = 0; i < n - 1; i++)
            {
                bool huboIntercambio = false;

                for (int j = 0; j < n - i - 1; j++)
                {
                    if (string.Compare(items[j], items[j + 1]) > 0)
                    {
                        // Intercambiar
                        string temp = items[j];
                        items[j] = items[j + 1];
                        items[j + 1] = temp;
                        huboIntercambio = true;
                    }
                }

                // Optimización: si no hay intercambios, la lista ya está ordenada
                if (!huboIntercambio)
                    break;
            }
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
                    var items = new List<string>();
                    foreach (var item in elemento.EnumerateArray())
                    {
                        string valor = ConvertirValorJson(item);
                        if (!string.IsNullOrEmpty(valor))
                        {
                            items.Add(valor);
                        }
                    }
                    return string.Join("; ", items);

                case JsonValueKind.Object:
                    // Para objetos anidados, crear una representación simple
                    var propiedades = new List<string>();
                    foreach (var prop in elemento.EnumerateObject())
                    {
                        propiedades.Add($"{prop.Name}: {ConvertirValorJson(prop.Value)}");
                    }
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
                bool estaVacia = true;

                foreach (DataRow row in _dataTable.Rows)
                {
                    if (!string.IsNullOrWhiteSpace(row[column].ToString()))
                    {
                        estaVacia = false;
                        break;
                    }
                }

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
                    for (int i = 0; i < _dataTable.Columns.Count; i++)
                    {
                        if (i > 0)
                            writer.Write(",");
                        writer.Write(_dataTable.Columns[i].ColumnName);
                    }
                    writer.WriteLine();

                    // Escribir datos
                    foreach (DataRow row in _dataTable.Rows)
                    {
                        for (int i = 0; i < row.ItemArray.Length; i++)
                        {
                            if (i > 0)
                                writer.Write(",");
                            writer.Write($"\"{row.ItemArray[i]}\"");
                        }
                        writer.WriteLine();
                    }
                }

                Console.WriteLine($"? Tabla exportada correctamente a: {csvFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al exportar a CSV: {ex.Message}");
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
            for (int i = 0; i < _dataTable.Columns.Count; i++)
            {
                Console.Write(_dataTable.Columns[i].ColumnName.PadRight(20) + "| ");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', 80));

            // Mostrar datos
            foreach (DataRow row in _dataTable.Rows)
            {
                for (int i = 0; i < row.ItemArray.Length; i++)
                {
                    Console.Write(row.ItemArray[i].ToString().PadRight(20).Substring(0, 20) + "| ");
                }
                Console.WriteLine();
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}