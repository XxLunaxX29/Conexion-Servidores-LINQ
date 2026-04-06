using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Xml.Linq;

namespace Conexion_Servidores_LINQ
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

                // LINQ: Obtener elementos con Where
                var elements = xmlDoc.Descendants()
                    .Where(e => e.Name.LocalName == elementName)
                    .ToList();

                if (elements.Count == 0)
                    throw new InvalidOperationException($"No se encontraron elementos con el nombre: {elementName}");

                Console.WriteLine($"? Se encontraron {elements.Count} elementos XML");

                // LINQ: Obtener todas las columnas posibles
                Console.WriteLine("? Extrayendo columnas...");
                var allColumns = ExtractAllColumns(elements);

                // LINQ: Ordenar columnas alfabéticamente con OrderBy
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
                    
                    // LINQ: Crear hash de fila de manera más eficiente
                    var rowData = sortedColumns
                        .Select(col => GetElementValue(element, col))
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
        /// Extrae todas las columnas únicas del conjunto de elementos XML usando LINQ.
        /// </summary>
        private List<string> ExtractAllColumns(List<XElement> elements)
        {
            return elements
                .SelectMany(element => element.Attributes()
                    .Select(attr => attr.Name.LocalName)
                    .Concat(element.Elements()
                        .Select(child => child.Name.LocalName)))
                .Distinct()
                .ToList();
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

                Console.WriteLine($"? Tabla exportada correctamente a: {csvFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"? Error al exportar a CSV: {ex.Message}");
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
