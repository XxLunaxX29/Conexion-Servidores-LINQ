using System;
using System.Collections.Generic;
using System.Data;
using System.Xml.Linq;
using System.Linq;

namespace ConexionServidores
{
    /// <summary>
    /// Clase que carga archivos XML y los estructura en una tabla evitando duplicados y columnas vacías.
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

                // Obtener elementos con LINQ
                var elements = xmlDoc.Descendants()
                    .Where(e => e.Name.LocalName == elementName)
                    .ToList();

                if (elements.Count == 0)
                    throw new InvalidOperationException($"No se encontraron elementos con el nombre: {elementName}");

                Console.WriteLine($"? Se encontraron {elements.Count} elementos XML");

                // Obtener todas las columnas posibles
                Console.WriteLine("? Extrayendo columnas...");
                var allColumns = ExtractAllColumns(elements);

                // Ordenar columnas alfabéticamente con LINQ
                var sortedColumns = allColumns.OrderBy(c => c).ToList();

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
                    var rowData = sortedColumns
                        .Select(column => GetElementValue(element, column))
                        .ToList();
                    
                    var rowHash = string.Join("|", rowData);

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
        /// Extrae todas las columnas únicas del conjunto de elementos XML.
        /// </summary>
        private List<string> ExtractAllColumns(List<XElement> elements)
        {
            var columns = elements
                .SelectMany(element => element.Attributes()
                    .Select(attr => attr.Name.LocalName)
                    .Concat(element.Elements()
                        .Select(child => child.Name.LocalName)))
                .Distinct()
                .ToList();

            return columns;
        }

        /// <summary>
        /// Obtiene el valor de un elemento específico considerando atributos y elementos hijo.
        /// </summary>
        private string GetElementValue(XElement element, string columnName)
        {
            // Buscar en atributos
            var attrValue = element.Attribute(columnName)?.Value?.Trim();
            if (!string.IsNullOrWhiteSpace(attrValue))
                return attrValue;

            // Buscar en elementos hijo
            var childValue = element.Element(columnName)?.Value?.Trim();
            if (!string.IsNullOrWhiteSpace(childValue))
                return childValue;

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
                    string cellValue = cell.ToString();
                    if (cellValue.Length > 20)
                        cellValue = cellValue.Substring(0, 17) + "...";
                    Console.Write(cellValue.PadRight(20) + "| ");
                }
                Console.WriteLine();
            }

            Console.WriteLine(new string('=', 80));
        }
    }
}