using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;

namespace ConexionServidores
{
    /// <summary>
    /// Clase que gestiona datos de un DataTable usando List<T> y Dictionary para acceso optimizado.
    /// </summary>
    public class DatasetManager
    {
        private List<dynamic> _datos;
        private Dictionary<string, List<int>> _indicesPorColumna;
        private DataTable _dataTable;
        private string _nombreDataset;

        public DatasetManager(string nombreDataset = "Dataset")
        {
            _datos = new List<dynamic>();
            _indicesPorColumna = new Dictionary<string, List<int>>();
            _nombreDataset = nombreDataset;
        }

        /// <summary>
        /// Carga datos desde un DataTable a la estructura de List y Dictionary.
        /// </summary>
        public void CargarDesdeDataTable(DataTable dataTable)
        {
            if (dataTable == null || dataTable.Rows.Count == 0)
                throw new ArgumentException("El DataTable está vacío o es nulo.");

            _dataTable = dataTable;
            _datos.Clear();
            _indicesPorColumna.Clear();

            Console.WriteLine($"\n? Cargando datos del DataTable en memoria...");
            Console.WriteLine($"? Total de filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}");

            // LINQ: Convertir cada fila del DataTable a un objeto dinámico
            int filasAgregadas = 0;
            foreach (DataRow row in dataTable.Rows)
            {
                var diccionarioFila = dataTable.Columns.Cast<DataColumn>()
                    .ToDictionary(column => column.ColumnName, column => row[column]);

                var objeto = ConvertirDiccionarioAObjeto(diccionarioFila);
                _datos.Add(objeto);

                // Agregar al índice por cada columna
                foreach (var kvp in diccionarioFila)
                {
                    string clave = $"{kvp.Key}:{kvp.Value}";
                    if (!_indicesPorColumna.ContainsKey(clave))
                    {
                        _indicesPorColumna[clave] = new List<int>();
                    }
                    _indicesPorColumna[clave].Add(filasAgregadas);
                }

                filasAgregadas++;
                if (filasAgregadas % 100 == 0)
                {
                    Console.Write($"\r? Procesadas {filasAgregadas} filas...");
                }
            }

            Console.WriteLine($"\r? Carga completada: {_datos.Count} filas almacenadas en memoria");
        }

        /// <summary>
        /// Convierte un diccionario a un objeto dinámico.
        /// </summary>
        private dynamic ConvertirDiccionarioAObjeto(Dictionary<string, object> diccionario)
        {
            dynamic objeto = new ExpandoObject();
            var diccionarioExpandido = (IDictionary<string, object>)objeto;

            foreach (var kvp in diccionario)
            {
                diccionarioExpandido[kvp.Key] = kvp.Value;
            }

            return objeto;
        }

        /// <summary>
        /// Obtiene todos los datos cargados.
        /// </summary>
        public List<dynamic> ObtenerTodos()
        {
            return new List<dynamic>(_datos);
        }

        /// <summary>
        /// Obtiene el total de registros cargados.
        /// </summary>
        public int ObtenerTotal()
        {
            return _datos.Count;
        }

        /// <summary>
        /// Busca datos por columna y valor usando el diccionario (acceso rápido).
        /// </summary>
        public List<dynamic> BuscarPorColumnaValor(string nombreColumna, object valor)
        {
            string clave = $"{nombreColumna}:{valor}";

            return _indicesPorColumna.ContainsKey(clave)
                ? _indicesPorColumna[clave].Select(i => _datos[i]).ToList()
                : new List<dynamic>();
        }

        /// <summary>
        /// Busca datos que contengan un valor en una columna (búsqueda parcial) usando LINQ.
        /// </summary>
        public List<dynamic> BuscarPorColumnaContiene(string nombreColumna, string valor)
        {
            return _datos
                .Where(item => ((ExpandoObject)item).Cast<KeyValuePair<string, object>>()
                    .Any(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase) &&
                        p.Value?.ToString()?.Contains(valor, StringComparison.OrdinalIgnoreCase) == true))
                .ToList();
        }

        /// <summary>
        /// Obtiene valores únicos de una columna usando LINQ.
        /// </summary>
        public List<object> ObtenerValoresUnicos(string nombreColumna)
        {
            return _datos
                .Select(item => ((ExpandoObject)item).Cast<KeyValuePair<string, object>>()
                    .FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value)
                .Where(valor => valor != null)
                .Distinct()
                .ToList();
        }

        /// <summary>
        /// Agrupa datos por una columna específica usando LINQ.
        /// </summary>
        public Dictionary<object, List<dynamic>> AgruparPor(string nombreColumna)
        {
            return _datos
                .GroupBy(item => ((ExpandoObject)item).Cast<KeyValuePair<string, object>>()
                    .FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value)
                .Where(g => g.Key != null)
                .ToDictionary(g => g.Key, g => g.ToList());
        }

        /// <summary>
        /// Obtiene estadísticas sobre los datos cargados usando LINQ.
        /// </summary>
        public Dictionary<string, object> ObtenerEstadisticas()
        {
            return new Dictionary<string, object>
            {
                { "TotalRegistros", _datos.Count },
                { "TotalColumnas", _dataTable?.Columns.Count ?? 0 },
                { "NombresColumnas", _dataTable?.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList() },
                { "TotalClaves", _indicesPorColumna.Count },
                { "MemoriaAproximada", ObtenerTamanoMemoria() }
            };
        }

        /// <summary>
        /// Obtiene una estimación del tamaño en memoria.
        /// </summary>
        private string ObtenerTamanoMemoria()
        {
            long bytes = GC.GetTotalMemory(false);

            return bytes < 1024
                ? $"{bytes} B"
                : bytes < 1024 * 1024
                ? $"{bytes / 1024} KB"
                : $"{bytes / (1024 * 1024)} MB";
        }

        /// <summary>
        /// Filtra datos usando un predicado usando LINQ.
        /// </summary>
        public List<dynamic> Filtrar(Func<dynamic, bool> predicado)
        {
            return _datos.Where(predicado).ToList();
        }

        /// <summary>
        /// Ordena datos por una columna específica usando LINQ.
        /// </summary>
        public List<dynamic> Ordenar(string nombreColumna, bool descendente = false)
        {
            return descendente
                ? _datos
                    .OrderByDescending(item => ObtenerValorColumna(item, nombreColumna))
                    .ToList()
                : _datos
                    .OrderBy(item => ObtenerValorColumna(item, nombreColumna))
                    .ToList();
        }

        /// <summary>
        /// Obtiene el valor de una columna de un objeto dinámico.
        /// </summary>
        private object ObtenerValorColumna(dynamic item, string nombreColumna)
        {
            try
            {
                var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>();
                return propiedades
                    .FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase))
                    .Value ?? 0;
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Obtiene un resumen de los datos con paginación usando LINQ.
        /// </summary>
        public List<dynamic> ObtenerPaginado(int numeroPagina, int registrosPorPagina)
        {
            return _datos
                .Skip((numeroPagina - 1) * registrosPorPagina)
                .Take(registrosPorPagina)
                .ToList();
        }

        /// <summary>
        /// Muestra información del dataset en la consola.
        /// </summary>
        public void MostrarInformacion()
        {
            var estadisticas = ObtenerEstadisticas();

            Console.WriteLine("\n?????????????????????????????????????????????");
            Console.WriteLine($"?  INFORMACIÓN DEL DATASET: {_nombreDataset}");
            Console.WriteLine("?????????????????????????????????????????????");
            Console.WriteLine($"?  Total de registros: {estadisticas["TotalRegistros"]}");
            Console.WriteLine($"?  Total de columnas: {estadisticas["TotalColumnas"]}");
            Console.WriteLine($"?  Total de claves en índices: {estadisticas["TotalClaves"]}");
            Console.WriteLine($"?  Memoria aproximada: {estadisticas["MemoriaAproximada"]}");
            Console.WriteLine("?????????????????????????????????????????????");

            if (estadisticas["NombresColumnas"] is List<string> columnas)
            {
                Console.WriteLine("\nColumnas disponibles:");
                int indice = 1;
                foreach (var col in columnas)
                {
                    Console.WriteLine($"  {indice}. {col}");
                    indice++;
                }
            }
        }

        /// <summary>
        /// Exporta los datos filtrados a un nuevo DataTable usando LINQ.
        /// </summary>
        public DataTable ExportarADataTable(List<dynamic> datos)
        {
            var dataTable = new DataTable();

            if (datos.Count == 0)
                return dataTable;

            // Obtener las propiedades del primer objeto
            var propiedadesEjemplo = ((ExpandoObject)datos[0]).Cast<KeyValuePair<string, object>>().ToList();

            foreach (var prop in propiedadesEjemplo)
            {
                dataTable.Columns.Add(prop.Key, typeof(object));
            }

            // LINQ: Llenar las filas
            foreach (var item in datos)
            {
                var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>().ToArray();
                dataTable.Rows.Add(propiedades.Select(p => p.Value).ToArray());
            }

            return dataTable;
        }

        /// <summary>
        /// Limpia todos los datos de la memoria.
        /// </summary>
        public void Limpiar()
        {
            _datos.Clear();
            _indicesPorColumna.Clear();
            GC.Collect();
            Console.WriteLine("? Datos limpiados de la memoria");
        }
    }
}
