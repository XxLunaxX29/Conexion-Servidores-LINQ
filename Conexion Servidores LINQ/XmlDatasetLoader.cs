using System;
using System.Collections.Generic;
using System.Data;
using System.Xml.Linq;
using System.Linq;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que carga archivos XML y los estructura en una tabla evitando duplicados y columnas vacías.
    /// Utiliza LINQ para búsqueda, ordenamiento y extracción de datos.
    /// </summary>
    public class XmlDatasetLoader
    {
        private DataTable _dataTable;

        public XmlDatasetLoader()
        {
            _dataTable = new DataTable();
        }

        /// <summary>
        /// Carga un archivo XML y lo convierte en una tabla de datos limpia.
        /// Utiliza LINQ para extraer elementos.
        /// </summary>
        /// <param name="xmlFilePath">Ruta del archivo XML</param>
        /// <param name="elementName">Nombre del elemento XML a procesar (ej: "row", "record")</param>
        /// <returns>DataTable con los datos cargados</returns>
        public DataTable LoadXmlFile(string xmlFilePath, string elementName = "row")
        {
            try
            {
                if (!File.Exists(xmlFilePath))
                    throw new FileNotFoundException($"El archivo XML no existe: {xmlFilePath}");

                Console.WriteLine("\n? Leyendo archivo XML...");
                var xmlDoc = XDocument.Load(xmlFilePath);
                _dataTable = new DataTable();

                // Obtener elementos usando LINQ
                var elements = xmlDoc.Descendants()
                    .Where(element => element.Name.LocalName == elementName)
                    .ToList();

                if (elements.Count == 0)
                    throw new InvalidOperationException($"No se encontraron elementos con el nombre: {elementName}");

                Console.WriteLine($"? Se encontraron {elements.Count} elementos XML");

                // Obtener todas las columnas posibles
                Console.WriteLine("? Extrayendo columnas...");
                var allColumns = ExtractAllColumns(elements);

                // Ordenar columnas alfabéticamente usando LINQ
                var sortedColumns = allColumns.OrderBy(col => col).ToList();

                // Crear columnas en la tabla
                foreach (var column in sortedColumns)
                {
                    _dataTable.Columns.Add(column, typeof(string));
                }

                Console.WriteLine($"? Se crearon {sortedColumns.Count} columnas");

                // Agregar filas evitando duplicados
                var addedRows = new HashSet<string>();
                int filasAgregadas = 0;
                int filasEnProceso = 0;

                Console.WriteLine("? Procesando filas...\n");

                foreach (var element in elements)
                {
                    filasEnProceso++;
                    var rowData = new List<string>();
                    var rowHash = "";

                    foreach (var column in sortedColumns)
                    {
                        var value = GetElementValue(element, column);
                        rowData.Add(value);
                        rowHash += value + "|";
                    }

                    // Verificar si la fila ya existe (evitar duplicados)
                    if (!addedRows.Contains(rowHash))
                    {
                        _dataTable.Rows.Add(rowData.ToArray());
                        addedRows.Add(rowHash);
                        filasAgregadas++;
                    }

                    // Mostrar progreso cada 10 filas
                    if (filasEnProceso % 10 == 0)
                    {
                        Console.Write($"\r? Procesadas {filasEnProceso} filas | Agregadas {filasAgregadas} filas");
                    }
                }

                Console.WriteLine($"\r? Carga completada: {filasEnProceso} leídas | {filasAgregadas} agregadas (sin duplicados)");

                return _dataTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al cargar el archivo XML: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Extrae todas las columnas únicas del conjunto de elementos XML usando LINQ.
        /// </summary>
        private List<string> ExtractAllColumns(List<XElement> elements)
        {
            var columns = elements
                .SelectMany(element => element.Attributes().Select(attr => attr.Name.LocalName))
                .Concat(elements.SelectMany(element => element.Elements().Select(child => child.Name.LocalName)))
                .Distinct()
                .ToList();

            return columns;
        }

        /// <summary>
        /// Obtiene el valor de un elemento específico considerando atributos y elementos hijo.
        /// </summary>
        private string GetElementValue(XElement element, string columnName)
        {
            // Buscar en atributos usando LINQ
            var attrValue = element.Attributes()
                .FirstOrDefault(attr => attr.Name.LocalName == columnName)
                ?.Value;

            if (!string.IsNullOrWhiteSpace(attrValue))
                return attrValue.Trim();

            // Buscar en elementos hijo usando LINQ
            var childValue = element.Elements()
                .FirstOrDefault(child => child.Name.LocalName == columnName)
                ?.Value;

            if (!string.IsNullOrWhiteSpace(childValue))
                return childValue.Trim();

            return string.Empty;
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

            // Mostrar datos
            foreach (DataRow row in _dataTable.Rows)
            {
                foreach (var cellValue in row.ItemArray.Select(item => 
                {
                    string value = item.ToString();
                    return value.Length > 20 ? value.Substring(0, 17) + "..." : value;
                }))
                {
                    Console.Write(cellValue.PadRight(20) + "| ");
                }
                Console.WriteLine();
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}