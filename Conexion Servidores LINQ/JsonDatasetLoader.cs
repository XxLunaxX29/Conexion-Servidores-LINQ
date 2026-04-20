using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Linq;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que carga archivos JSON y los estructura en una tabla evitando duplicados y columnas vacías.
    /// Utiliza LINQ para búsqueda, ordenamiento y extracción de datos.
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
        /// Utiliza LINQ para extraer elementos.
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
                        elementos.AddRange(root.EnumerateArray());
                    }
                    else if (root.ValueKind == JsonValueKind.Object)
                    {
                        // Buscar arrays dentro del objeto usando LINQ
                        var arrayProperty = root.EnumerateObject()
                            .FirstOrDefault(prop => prop.Value.ValueKind == JsonValueKind.Array);

                        if (arrayProperty.Value.ValueKind == JsonValueKind.Array)
                        {
                            elementos.AddRange(arrayProperty.Value.EnumerateArray());
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

                    // Ordenar columnas alfabéticamente usando LINQ
                    var sortedClaves = todasLasClaves.OrderBy(clave => clave).ToList();

                    // Crear columnas en la tabla
                    foreach (var clave in sortedClaves)
                    {
                        _dataTable.Columns.Add(clave, typeof(string));
                    }

                    Console.WriteLine($"? Se crearon {sortedClaves.Count} columnas");

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

                        foreach (var clave in sortedClaves)
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
        /// Extrae todas las claves únicas del conjunto de elementos JSON usando LINQ.
        /// </summary>
        private List<string> ExtraerTodasLasClaves(List<JsonElement> elementos)
        {
            var claves = elementos
                .Where(elemento => elemento.ValueKind == JsonValueKind.Object)
                .SelectMany(elemento => elemento.EnumerateObject().Select(prop => prop.Name))
                .Distinct()
                .ToList();

            return claves;
        }

        /// <summary>
        /// Obtiene el valor de una propiedad específica del elemento JSON usando LINQ.
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
        /// Utiliza LINQ para colecciones.
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

                JsonValueKind.Array => string.Join("; ", 
                    elemento.EnumerateArray()
                        .Select(ConvertirValorJson)
                        .Where(v => !string.IsNullOrEmpty(v))),

                JsonValueKind.Object => "{" + string.Join(", ", 
                    elemento.EnumerateObject()
                        .Select(prop => $"{prop.Name}: {ConvertirValorJson(prop.Value)}")) + "}",

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
        /// Filtra filas de la tabla usando LINQ .Where().
        /// </summary>
        /// <param name="columnName">Nombre de la columna a filtrar</param>
        /// <param name="filterValue">Valor o patrón a buscar</param>
        /// <param name="exactMatch">Si es true, busca coincidencia exacta; si es false, busca coincidencia parcial</param>
        /// <returns>DataTable filtrado</returns>
        public DataTable FilterByColumn(string columnName, string filterValue, bool exactMatch = false)
        {
            try
            {
                if (!_dataTable.Columns.Contains(columnName))
                    throw new ArgumentException($"La columna '{columnName}' no existe en la tabla.");

                if (string.IsNullOrWhiteSpace(filterValue))
                    throw new ArgumentException("El valor de filtro no puede estar vacío.");

                var filteredRows = exactMatch
                    ? _dataTable.AsEnumerable()
                        .Where(row => row[columnName].ToString().Equals(filterValue, StringComparison.OrdinalIgnoreCase))
                        .ToList()
                    : _dataTable.AsEnumerable()
                        .Where(row => row[columnName].ToString().Contains(filterValue, StringComparison.OrdinalIgnoreCase))
                        .ToList();

                var resultTable = _dataTable.Clone();
                foreach (var row in filteredRows)
                {
                    resultTable.ImportRow(row);
                }

                Console.WriteLine($"✓ Filtrado completado: {filteredRows.Count} filas encontradas de {_dataTable.Rows.Count}");
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al filtrar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Filtra múltiples criterios en la tabla usando LINQ .Where().
        /// </summary>
        /// <param name="filters">Diccionario con pares columna-valor</param>
        /// <param name="exactMatch">Si es true, busca coincidencia exacta</param>
        /// <returns>DataTable filtrado</returns>
        public DataTable FilterByMultipleCriteria(Dictionary<string, string> filters, bool exactMatch = false)
        {
            try
            {
                if (filters == null || filters.Count == 0)
                    throw new ArgumentException("Debe proporcionar al menos un filtro.");

                var filteredRows = _dataTable.AsEnumerable();

                foreach (var filter in filters)
                {
                    if (!_dataTable.Columns.Contains(filter.Key))
                        throw new ArgumentException($"La columna '{filter.Key}' no existe en la tabla.");

                    filteredRows = exactMatch
                        ? filteredRows.Where(row => row[filter.Key].ToString().Equals(filter.Value, StringComparison.OrdinalIgnoreCase))
                        : filteredRows.Where(row => row[filter.Key].ToString().Contains(filter.Value, StringComparison.OrdinalIgnoreCase));
                }

                var resultList = filteredRows.ToList();
                var resultTable = _dataTable.Clone();

                foreach (var row in resultList)
                {
                    resultTable.ImportRow(row);
                }

                Console.WriteLine($"✓ Filtrado completado: {resultList.Count} filas encontradas de {_dataTable.Rows.Count}");
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al filtrar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Ordena la tabla usando LINQ .OrderBy() o .OrderByDescending().
        /// </summary>
        /// <param name="columnName">Nombre de la columna por la que ordenar</param>
        /// <param name="descending">Si es true, ordena de forma descendente</param>
        /// <returns>DataTable ordenado</returns>
        public DataTable SortByColumn(string columnName, bool descending = false)
        {
            try
            {
                if (!_dataTable.Columns.Contains(columnName))
                    throw new ArgumentException($"La columna '{columnName}' no existe en la tabla.");

                var sortedRows = descending
                    ? _dataTable.AsEnumerable()
                        .OrderByDescending(row => row[columnName].ToString())
                        .ToList()
                    : _dataTable.AsEnumerable()
                        .OrderBy(row => row[columnName].ToString())
                        .ToList();

                var resultTable = _dataTable.Clone();
                foreach (var row in sortedRows)
                {
                    resultTable.ImportRow(row);
                }

                Console.WriteLine($"✓ Ordenamiento completado: {sortedRows.Count} filas ordenadas por '{columnName}'");
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al ordenar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Ordena la tabla por múltiples columnas usando LINQ .OrderBy().
        /// </summary>
        /// <param name="columnNames">Lista de nombres de columnas para ordenamiento (en orden de prioridad)</param>
        /// <param name="descending">Si es true, ordena todas las columnas de forma descendente</param>
        /// <returns>DataTable ordenado</returns>
        public DataTable SortByMultipleColumns(List<string> columnNames, bool descending = false)
        {
            try
            {
                if (columnNames == null || columnNames.Count == 0)
                    throw new ArgumentException("Debe proporcionar al menos una columna para ordenar.");

                foreach (var columnName in columnNames)
                {
                    if (!_dataTable.Columns.Contains(columnName))
                        throw new ArgumentException($"La columna '{columnName}' no existe en la tabla.");
                }

                var sortedRows = _dataTable.AsEnumerable();

                // Aplicar ordenamiento por cada columna en orden inverso para mantener prioridad
                for (int i = columnNames.Count - 1; i >= 0; i--)
                {
                    sortedRows = descending
                        ? sortedRows.OrderByDescending(row => row[columnNames[i]].ToString())
                        : sortedRows.OrderBy(row => row[columnNames[i]].ToString());
                }

                var resultTable = _dataTable.Clone();
                foreach (var row in sortedRows.ToList())
                {
                    resultTable.ImportRow(row);
                }

                Console.WriteLine($"✓ Ordenamiento completado: {sortedRows.Count()} filas ordenadas por '{string.Join(", ", columnNames)}'");
                return resultTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al ordenar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Agrupa filas de la tabla usando LINQ .GroupBy().
        /// </summary>
        /// <param name="columnName">Nombre de la columna por la que agrupar</param>
        /// <returns>Diccionario con grupos y sus filas</returns>
        public Dictionary<object, DataTable> GroupByColumn(string columnName)
        {
            try
            {
                if (!_dataTable.Columns.Contains(columnName))
                    throw new ArgumentException($"La columna '{columnName}' no existe en la tabla.");

                var groupedData = _dataTable.AsEnumerable()
                    .GroupBy(row => row[columnName])
                    .ToDictionary(group => group.Key, group => group.ToList());

                var resultGroups = new Dictionary<object, DataTable>();

                foreach (var group in groupedData)
                {
                    var groupTable = _dataTable.Clone();
                    foreach (var row in group.Value)
                    {
                        groupTable.ImportRow(row);
                    }
                    resultGroups[group.Key] = groupTable;
                }

                Console.WriteLine($"✓ Agrupamiento completado: {resultGroups.Count} grupos creados por '{columnName}'");
                return resultGroups;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al agrupar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Agrupa filas por múltiples columnas usando LINQ .GroupBy().
        /// </summary>
        /// <param name="columnNames">Lista de nombres de columnas para agrupar</param>
        /// <returns>Diccionario con grupos compuestos y sus filas</returns>
        public Dictionary<string, DataTable> GroupByMultipleColumns(List<string> columnNames)
        {
            try
            {
                if (columnNames == null || columnNames.Count == 0)
                    throw new ArgumentException("Debe proporcionar al menos una columna para agrupar.");

                foreach (var columnName in columnNames)
                {
                    if (!_dataTable.Columns.Contains(columnName))
                        throw new ArgumentException($"La columna '{columnName}' no existe en la tabla.");
                }

                var groupedData = _dataTable.AsEnumerable()
                    .GroupBy(row => string.Join("|", columnNames.Select(col => row[col].ToString())))
                    .ToDictionary(group => group.Key, group => group.ToList());

                var resultGroups = new Dictionary<string, DataTable>();

                foreach (var group in groupedData)
                {
                    var groupTable = _dataTable.Clone();
                    foreach (var row in group.Value)
                    {
                        groupTable.ImportRow(row);
                    }
                    resultGroups[group.Key] = groupTable;
                }

                Console.WriteLine($"✓ Agrupamiento completado: {resultGroups.Count} grupos creados por '{string.Join(", ", columnNames)}'");
                return resultGroups;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error al agrupar: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Obtiene estadísticas de un agrupamiento.
        /// </summary>
        /// <param name="groupedData">Diccionario de grupos</param>
        /// <returns>Información de conteo por grupo</returns>
        public Dictionary<object, int> GetGroupStatistics(Dictionary<object, DataTable> groupedData)
        {
            return groupedData.ToDictionary(g => g.Key, g => g.Value.Rows.Count);
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
                    // Escribir encabezados usando LINQ
                    var headers = string.Join(",", 
                        _dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
                    writer.WriteLine(headers);

                    // Escribir datos usando LINQ
                    foreach (var values in _dataTable.Rows.Cast<DataRow>()
                        .Select(row => string.Join(",", 
                            row.ItemArray.Select(item => $"\"{item}\""))))
                    {
                        writer.WriteLine(values);
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
        /// Utiliza LINQ para renderizar encabezados y datos.
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

            // Mostrar encabezados usando LINQ
            foreach (var columnName in _dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName))
            {
                Console.Write(columnName.PadRight(20) + "| ");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', 80));

            // Mostrar datos usando LINQ
            foreach (var values in _dataTable.Rows.Cast<DataRow>()
                .Select(row => row.ItemArray
                    .Select(item =>
                    {
                        string value = item.ToString();
                        return value.Length > 20 ? value.Substring(0, 17) + "..." : value;
                    })))
            {
                foreach (var cellValue in values)
                {
                    Console.Write(cellValue.PadRight(20) + "| ");
                }
                Console.WriteLine();
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}