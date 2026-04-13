using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que gestiona datos de un DataTable usando List<T> y Dictionary para acceso optimizado.
    /// </summary>
    public class DatasetManager
    {
        private List<dynamic> _datos;
        private Dictionary<string, List<int>> _indicesPorColumna; // Almacena índices en lugar de objetos
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

            // Convertir cada fila del DataTable a un objeto dinámico
            int filasAgregadas = 0;

            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                DataRow row = dataTable.Rows[i];
                var diccionarioFila = new Dictionary<string, object>();

                foreach (DataColumn column in dataTable.Columns)
                {
                    diccionarioFila[column.ColumnName] = row[column];
                }

                // Convertir a un objeto dinámico (ExpandoObject)
                var objeto = ConvertirDiccionarioAObjeto(diccionarioFila);
                _datos.Add(objeto);
                filasAgregadas++;

                // Agregar al índice por cada columna
                foreach (var columna in dataTable.Columns.Cast<DataColumn>())
                {
                    string clave = $"{columna.ColumnName}:{row[columna]}";

                    if (!_indicesPorColumna.ContainsKey(clave))
                    {
                        _indicesPorColumna[clave] = new List<int>();
                    }
                    _indicesPorColumna[clave].Add(i); // Almacenar índice
                }

                if (filasAgregadas % 100 == 0)
                {
                    Console.Write($"\r? Procesadas {filasAgregadas} filas...");
                }
            }

            Console.WriteLine($"\r? Carga completada: {filasAgregadas} filas almacenadas en memoria");
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

            if (_indicesPorColumna.ContainsKey(clave))
            {
                var indices = _indicesPorColumna[clave];
                return indices.Select(i => _datos[i]).ToList();
            }

            return new List<dynamic>();
        }

        /// <summary>
        /// Busca datos que contengan un valor en una columna (búsqueda parcial).
        /// </summary>
        public List<dynamic> BuscarPorColumnaContiene(string nombreColumna, string valor)
        {
            var resultado = new List<dynamic>();

            foreach (var item in _datos)
            {
                var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>();

                if (propiedades.Any(p =>
                    p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase) &&
                    p.Value?.ToString()?.Contains(valor, StringComparison.OrdinalIgnoreCase) == true))
                {
                    resultado.Add(item);
                }
            }

            return resultado;
        }

        /// <summary>
        /// Obtiene valores únicos de una columna.
        /// </summary>
        public List<object> ObtenerValoresUnicos(string nombreColumna)
        {
            var valoresUnicos = new HashSet<object>();

            foreach (var item in _datos)
            {
                var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>();
                var valor = propiedades.FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value;

                if (valor != null)
                {
                    valoresUnicos.Add(valor);
                }
            }

            return valoresUnicos.ToList();
        }

        /// <summary>
        /// Agrupa datos por una columna específica.
        /// </summary>
        public Dictionary<object, List<dynamic>> AgruparPor(string nombreColumna)
        {
            var grupos = new Dictionary<object, List<dynamic>>();

            foreach (var item in _datos)
            {
                var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>();
                var clave = propiedades.FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value;

                if (clave != null)
                {
                    if (!grupos.ContainsKey(clave))
                    {
                        grupos[clave] = new List<dynamic>();
                    }
                    grupos[clave].Add(item);
                }
            }

            return grupos;
        }

        /// <summary>
        /// Obtiene estadísticas sobre los datos cargados.
        /// </summary>
        public Dictionary<string, object> ObtenerEstadisticas()
        {
            var estadisticas = new Dictionary<string, object>
            {
                { "TotalRegistros", _datos.Count },
                { "TotalColumnas", _dataTable?.Columns.Count ?? 0 },
                { "NombresColumnas", _dataTable?.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList() },
                { "TotalClaves", _indicesPorColumna.Count },
                { "MemoriaAproximada", ObtenerTamanoMemoria() }
            };

            return estadisticas;
        }

        /// <summary>
        /// Obtiene una estimación del tamaño en memoria.
        /// </summary>
        private string ObtenerTamanoMemoria()
        {
            long bytes = GC.GetTotalMemory(false);

            if (bytes < 1024)
                return $"{bytes} B";
            else if (bytes < 1024 * 1024)
                return $"{bytes / 1024} KB";
            else
                return $"{bytes / (1024 * 1024)} MB";
        }

        /// <summary>
        /// Filtra datos usando un predicado.
        /// </summary>
        public List<dynamic> Filtrar(Func<dynamic, bool> predicado)
        {
            return _datos.Where(predicado).ToList();
        }

        /// <summary>
        /// Ordena datos por una columna específica.
        /// </summary>
        public List<dynamic> Ordenar(string nombreColumna, bool descendente = false)
        {
            var resultado = new List<dynamic>(_datos);

            resultado.Sort((a, b) =>
            {
                var propiedadesA = ((ExpandoObject)a).Cast<KeyValuePair<string, object>>();
                var propiedadesB = ((ExpandoObject)b).Cast<KeyValuePair<string, object>>();

                var valorA = propiedadesA.FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value;
                var valorB = propiedadesB.FirstOrDefault(p => p.Key.Equals(nombreColumna, StringComparison.OrdinalIgnoreCase)).Value;

                int comparacion = ((IComparable)valorA)?.CompareTo(valorB) ?? 0;

                return descendente ? -comparacion : comparacion;
            });

            return resultado;
        }

        /// <summary>
        /// Obtiene un resumen de los datos con paginación.
        /// </summary>
        public List<dynamic> ObtenerPaginado(int numeroPagina, int registrosPorPagina)
        {
            int salto = (numeroPagina - 1) * registrosPorPagina;
            return _datos.Skip(salto).Take(registrosPorPagina).ToList();
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
                for (int i = 0; i < columnas.Count; i++)
                {
                    Console.WriteLine($"  {i + 1}. {columnas[i]}");
                }
            }
        }

        /// <summary>
        /// Exporta los datos filtrados a un nuevo DataTable.
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

            // Llenar las filas
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