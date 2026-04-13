using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que carga archivos CSV y los estructura en una tabla evitando duplicados y columnas vacías.
    /// </summary>
    public class CsvDatasetLoader
    {
        private DataTable _dataTable;

        public CsvDatasetLoader()
        {
            _dataTable = new DataTable();
        }

        /// <summary>
        /// Carga un archivo CSV y lo convierte en una tabla de datos limpia.
        /// </summary>
        /// <param name="csvFilePath">Ruta del archivo CSV</param>
        /// <param name="tieneEncabezado">Indica si la primera fila contiene encabezados</param>
        /// <returns>DataTable con los datos cargados</returns>
        public DataTable LoadCsvFile(string csvFilePath, bool tieneEncabezado = true)
        {
            try
            {
                if (!File.Exists(csvFilePath))
                    throw new FileNotFoundException($"El archivo CSV no existe: {csvFilePath}");

                Console.WriteLine("\n? Leyendo archivo CSV...");
                _dataTable = new DataTable();
                var lineas = File.ReadAllLines(csvFilePath);

                if (lineas.Length == 0)
                    throw new InvalidOperationException("El archivo CSV está vacío.");

                Console.WriteLine($"? Se encontraron {lineas.Length} líneas");

                // Procesar encabezados
                Console.WriteLine("? Procesando encabezados...");
                string[] encabezados = tieneEncabezado
                    ? ParsearLinea(lineas[0])
                    : GenerarEncabezadosAutomaticos(ParsearLinea(lineas[0]).Length);

                // Crear columnas
                foreach (var encabezado in encabezados)
                {
                    if (!string.IsNullOrWhiteSpace(encabezado))
                        _dataTable.Columns.Add(encabezado.Trim(), typeof(string));
                }

                Console.WriteLine($"? Se crearon {encabezados.Length} columnas");

                // Procesar datos y eliminar duplicados
                var addedRows = new HashSet<string>();
                int inicio = tieneEncabezado ? 1 : 0;
                int filasEnProceso = 0;
                int filasAgregadas = 0;

                Console.WriteLine("? Procesando filas de datos...\n");

                for (int i = inicio; i < lineas.Length; i++)
                {
                    filasEnProceso++;
                    string[] valores = ParsearLinea(lineas[i]);

                    // Asegurar que tiene el mismo número de columnas
                    if (valores.Length != encabezados.Length)
                        continue;

                    var rowData = valores.Select(v => v.Trim()).ToArray();
                    var rowHash = string.Join("|", rowData);

                    // Evitar duplicados
                    if (!addedRows.Contains(rowHash))
                    {
                        _dataTable.Rows.Add(rowData);
                        addedRows.Add(rowHash);
                        filasAgregadas++;
                    }

                    // Mostrar progreso cada 10 filas
                    if (filasEnProceso % 10 == 0)
                    {
                        Console.Write($"\r? Procesadas {filasEnProceso} filas | Agregadas {filasAgregadas} filas");
                    }
                }

                // Eliminar columnas completamente vacías
                RemoverColumnasVacias();

                Console.WriteLine($"\r? Carga completada: {filasEnProceso} leídas | {filasAgregadas} agregadas (sin duplicados)");

                return _dataTable;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al cargar el archivo CSV: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Parsea una línea CSV considerando valores entrecomillados.
        /// </summary>
        private string[] ParsearLinea(string linea)
        {
            var valores = new List<string>();
            var actual = new StringBuilder();
            bool entrecomillas = false;

            foreach (char c in linea)
            {
                if (c == '"')
                {
                    entrecomillas = !entrecomillas;
                }
                else if (c == ',' && !entrecomillas)
                {
                    valores.Add(actual.ToString());
                    actual.Clear();
                }
                else
                {
                    actual.Append(c);
                }
            }

            valores.Add(actual.ToString());
            return valores.ToArray();
        }

        /// <summary>
        /// Genera encabezados automáticos (Columna1, Columna2, etc.)
        /// </summary>
        private string[] GenerarEncabezadosAutomaticos(int cantidad)
        {
            return Enumerable.Range(1, cantidad)
                .Select(i => $"Columna{i}")
                .ToArray();
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