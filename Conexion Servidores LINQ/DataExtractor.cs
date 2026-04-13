using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;

namespace Conexion_Servidores_LINQ
{
    /// <summary>
    /// Clase que extrae información primordial de los datos cargados (id, nombre, categoría, valor, cantidad, precioUnitario).
    /// Normaliza campos independientemente de la fuente de datos con sinónimos adaptados.
    /// </summary>
    public class DataExtractor
    {
        private DataTable _dataTable;
        private Dictionary<string, string> _mapeoColumnas; // Mapea nombres "estándar" a nombres reales de columnas

        public DataExtractor()
        {
            _mapeoColumnas = new Dictionary<string, string>();
        }

        /// <summary>
        /// Configura el extractor con un DataTable y detecta automáticamente los campos primordiales.
        /// </summary>
        public void ConfigurarConDataTable(DataTable dataTable)
        {
            _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));
            DetectarColumnasAutomaticamente();
        }

        /// <summary>
        /// Detecta automáticamente las columnas que corresponden a id, nombre, categoría, valor, cantidad y precioUnitario.
        /// </summary>
        private void DetectarColumnasAutomaticamente()
        {
            _mapeoColumnas.Clear();

            var nombresColumnasMinusculas = _dataTable.Columns.Cast<DataColumn>()
                .ToDictionary(c => c.ColumnName.ToLower(), c => c.ColumnName);

            // Definir mapeos de campos estándar con sus sinónimos
            var camposEstandar = new Dictionary<string, string[]>
            {
                { "id", new[] { "id", "id_venta", "venta", "identificador", "codigo", "code" } },
                { "nombre", new[] { "nombre", "nombre_producto", "producto", "titulo", "title", "name", "descripcion", "description" } },
                { "categoria", new[] { "categoria", "categoria_producto", "category", "grupo", "tipo", "type", "clase", "class" } },
                { "cantidad", new[] { "cantidad", "qty", "quantity", "unidades" } },
                { "preciounitario", new[] { "precio_unitario", "preciounitario", "precio_unit", "preciou", "unit_price", "unitprice", "precio" } },
                { "valor", new[] { "total", "total_venta", "precio_unitario", "precio", "valor", "value", "amount" } }
            };

            // Detectar cada campo usando LINQ
            foreach (var campo in camposEstandar)
            {
                var nombreColumnaDetectado = DetectarColumna(nombresColumnasMinusculas, campo.Value);
                if (nombreColumnaDetectado != null)
                {
                    _mapeoColumnas[campo.Key] = nombreColumnaDetectado;
                }
            }

            MostrarResultadosDeteccion();
        }

        /// <summary>
        /// Detecta una columna buscando coincidencias exactas primero, luego parciales.
        /// </summary>
        private string DetectarColumna(Dictionary<string, string> nombresColumnasMinusculas, string[] sinonimos)
        {
            // Buscar coincidencia exacta con LINQ
            var coincidenciaExacta = sinonimos
                .Select(s => nombresColumnasMinusculas.FirstOrDefault(nc => nc.Key == s).Value)
                .FirstOrDefault(resultado => resultado != null);

            if (coincidenciaExacta != null)
                return coincidenciaExacta;

            // Buscar coincidencia parcial con LINQ
            var coincidenciaParci = sinonimos
                .SelectMany(s => nombresColumnasMinusculas
                    .Where(nc => nc.Key.Contains(s))
                    .Select(nc => nc.Value))
                .FirstOrDefault();

            return coincidenciaParci;
        }

        /// <summary>
        /// Muestra los resultados de la detección automática en consola.
        /// </summary>
        private void MostrarResultadosDeteccion()
        {
            Console.WriteLine("\n??????????????????????????????????????????");
            Console.WriteLine("?  DETECCIÓN DE CAMPOS PRIMORDIALES      ?");
            Console.WriteLine("??????????????????????????????????????????\n");

            if (_mapeoColumnas.Count == 0)
            {
                Console.WriteLine("? No se detectaron campos primordiales.");
                Console.WriteLine("Columnas disponibles en el DataTable:");
                foreach (var item in _dataTable.Columns.Cast<DataColumn>()
                    .Select((col, index) => new { Index = index + 1, Column = col.ColumnName }))
                {
                    Console.WriteLine($"  {item.Index}. {item.Column}");
                }
            }
            else
            {
                foreach (var mapeo in _mapeoColumnas)
                {
                    Console.WriteLine($"? {mapeo.Key.ToUpper()} ? '{mapeo.Value}'");
                }
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Permite mapear manualmente un campo estándar a una columna específica.
        /// </summary>
        public void MapearColumnaPersonalizada(string campoEstandar, string nombreColumnaReal)
        {
            if (!_dataTable.Columns.Contains(nombreColumnaReal))
                throw new ArgumentException($"La columna '{nombreColumnaReal}' no existe en el DataTable.");

            _mapeoColumnas[campoEstandar.ToLower()] = nombreColumnaReal;
            Console.WriteLine($"? Mapeo personalizado: {campoEstandar} ? {nombreColumnaReal}");
        }

        /// <summary>
        /// Extrae los datos primordiales del DataTable usando LINQ.
        /// </summary>
        public List<ProductoPrimordial> ExtraerDatos()
        {
            if (_dataTable == null)
                throw new InvalidOperationException("El DataTable no ha sido configurado. Debe llamar a ConfigurarConDataTable() primero.");

            return _dataTable.AsEnumerable()
                .Select(row => new ProductoPrimordial
                {
                    Id = ObtenerValorString(row, "id"),
                    Nombre = ObtenerValorString(row, "nombre"),
                    Categoria = ObtenerValorString(row, "categoria"),
                    Valor = ObtenerValorDecimal(row, "valor"),
                    Cantidad = ObtenerValorInt(row, "cantidad"),
                    PrecioUnitario = ObtenerValorDecimal(row, "preciounitario")
                })
                .Where(p => !string.IsNullOrEmpty(p.Id) || !string.IsNullOrEmpty(p.Nombre))
                .ToList();
        }

        /// <summary>
        /// Extrae datos primordiales desde una lista de objetos dinámicos (ExpandoObject) usando LINQ.
        /// </summary>
        public List<ProductoPrimordial> ExtraerDatos(List<dynamic> datos)
        {
            if (datos == null || datos.Count == 0)
                throw new ArgumentException("La lista de datos no puede estar vacía.", nameof(datos));

            return datos
                .Select(item =>
                {
                    var propiedades = ((ExpandoObject)item).Cast<KeyValuePair<string, object>>().ToList();

                    return new ProductoPrimordial
                    {
                        Id = ObtenerValorDeDictionary(propiedades, new[] { "id", "id_venta", "venta", "identificador", "codigo", "code" }),
                        Nombre = ObtenerValorDeDictionary(propiedades, new[] { "nombre", "nombre_producto", "producto", "titulo", "title", "name", "descripcion", "description" }),
                        Categoria = ObtenerValorDeDictionary(propiedades, new[] { "categoria", "categoria_producto", "category", "grupo", "tipo", "type", "clase", "class" }),
                        Cantidad = ObtenerValorIntDeDictionary(propiedades, new[] { "cantidad", "qty", "quantity", "unidades" }),
                        PrecioUnitario = ObtenerValorDecimalDeDictionary(propiedades, new[] { "precio_unitario", "preciounitario", "precio_unit", "preciou", "unit_price", "unitprice", "precio" }),
                        Valor = ObtenerValorDecimalDeDictionary(propiedades, new[] { "total", "total_venta", "precio_unitario", "precio", "valor", "value", "amount" })
                    };
                })
                .Where(p => !string.IsNullOrEmpty(p.Id) || !string.IsNullOrEmpty(p.Nombre))
                .ToList();
        }

        /// <summary>
        /// Obtiene un valor string de una fila del DataTable, usando el mapeo de columnas.
        /// </summary>
        private string ObtenerValorString(DataRow row, string campoEstandar)
        {
            return _mapeoColumnas.TryGetValue(campoEstandar, out var columna)
                ? row[columna]?.ToString()
                : string.Empty;
        }

        /// <summary>
        /// Obtiene un valor decimal de una fila del DataTable, usando el mapeo de columnas.
        /// </summary>
        private decimal ObtenerValorDecimal(DataRow row, string campoEstandar)
        {
            if (!_mapeoColumnas.TryGetValue(campoEstandar, out var columna))
                return 0;

            return decimal.TryParse(row[columna]?.ToString(), out var valor) ? valor : 0;
        }

        /// <summary>
        /// Obtiene un valor int de una fila del DataTable, usando el mapeo de columnas.
        /// </summary>
        private int ObtenerValorInt(DataRow row, string campoEstandar)
        {
            if (!_mapeoColumnas.TryGetValue(campoEstandar, out var columna))
                return 0;

            return int.TryParse(row[columna]?.ToString(), out var valor) ? valor : 0;
        }

        /// <summary>
        /// Obtiene un valor string desde un diccionario de propiedades, buscando por sinónimos con LINQ.
        /// </summary>
        private string ObtenerValorDeDictionary(List<KeyValuePair<string, object>> propiedades, string[] terminos)
        {
            return propiedades
                .FirstOrDefault(p => terminos.Any(t => p.Key.ToLower() == t || p.Key.ToLower().Contains(t)))
                .Value?.ToString() ?? string.Empty;
        }

        /// <summary>
        /// Obtiene un valor decimal desde un diccionario de propiedades, buscando por sinónimos con LINQ.
        /// </summary>
        private decimal ObtenerValorDecimalDeDictionary(List<KeyValuePair<string, object>> propiedades, string[] terminos)
        {
            var valor = propiedades
                .FirstOrDefault(p => terminos.Any(t => p.Key.ToLower() == t || p.Key.ToLower().Contains(t)))
                .Value?.ToString();

            return decimal.TryParse(valor, out var resultado) ? resultado : 0;
        }

        /// <summary>
        /// Obtiene un valor int desde un diccionario de propiedades, buscando por sinónimos con LINQ.
        /// </summary>
        private int ObtenerValorIntDeDictionary(List<KeyValuePair<string, object>> propiedades, string[] terminos)
        {
            var valor = propiedades
                .FirstOrDefault(p => terminos.Any(t => p.Key.ToLower() == t || p.Key.ToLower().Contains(t)))
                .Value?.ToString();

            return int.TryParse(valor, out var resultado) ? resultado : 0;
        }

        /// <summary>
        /// Obtiene el mapeo de columnas actual.
        /// </summary>
        public Dictionary<string, string> ObtenerMapeo()
        {
            return new Dictionary<string, string>(_mapeoColumnas);
        }

        /// <summary>
        /// Limpia el mapeo de columnas.
        /// </summary>
        public void LimpiarMapeo()
        {
            _mapeoColumnas.Clear();
        }

        /// <summary>
        /// Muestra los datos primordiales en formato tabular en la consola.
        /// </summary>
        public void MostrarDatosPrimordiales(List<ProductoPrimordial> productos)
        {
            if (productos.Count == 0)
            {
                Console.WriteLine("\n? No hay datos para mostrar.");
                return;
            }

            Console.WriteLine("\n????????????????????????????????????????????????????????????????????????????????????????");
            Console.WriteLine($"?  DATOS PRIMORDIALES: {productos.Count} registros");
            Console.WriteLine("????????????????????????????????????????????????????????????????????????????????????????\n");

            // Encabezados
            Console.WriteLine($"{"ID",-12} | {"NOMBRE",-20} | {"CATEGORÍA",-15} | {"VALOR",-12} | {"CANTIDAD",-10} | {"P.UNITARIO",-12}");
            Console.WriteLine(new string('-', 95));

            // Mostrar datos
            foreach (var producto in productos)
            {
                string id = producto.Id ?? "N/A";
                string nombre = producto.Nombre ?? "N/A";
                string categoria = producto.Categoria ?? "N/A";
                string valor = producto.Valor > 0 ? $"${producto.Valor:F2}" : "N/A";
                string cantidad = producto.Cantidad > 0 ? producto.Cantidad.ToString() : "N/A";
                string precioUnitario = producto.PrecioUnitario > 0 ? $"${producto.PrecioUnitario:F2}" : "N/A";

                Console.WriteLine($"{id,-12} | {nombre,-20} | {categoria,-15} | {valor,-12} | {cantidad,-10} | {precioUnitario,-12}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Exporta los datos primordiales a un nuevo DataTable usando LINQ.
        /// </summary>
        public DataTable ExportarADataTable(List<ProductoPrimordial> productos)
        {
            var dataTable = new DataTable("DatosPrimordiales");

            dataTable.Columns.Add("ID", typeof(string));
            dataTable.Columns.Add("Nombre", typeof(string));
            dataTable.Columns.Add("Categoria", typeof(string));
            dataTable.Columns.Add("Valor", typeof(decimal));
            dataTable.Columns.Add("Cantidad", typeof(int));
            dataTable.Columns.Add("PrecioUnitario", typeof(decimal));

            foreach (var producto in productos)
            {
                dataTable.Rows.Add(producto.Id, producto.Nombre, producto.Categoria, producto.Valor, producto.Cantidad, producto.PrecioUnitario);
            }

            return dataTable;
        }
    }

    /// <summary>
    /// Clase que representa un producto con datos primordiales.
    /// </summary>
    public class ProductoPrimordial
    {
        public string Id { get; set; }
        public string Nombre { get; set; }
        public string Categoria { get; set; }
        public decimal Valor { get; set; }
        public int Cantidad { get; set; }
        public decimal PrecioUnitario { get; set; }

        public override string ToString()
        {
            return $"ID: {Id}, Nombre: {Nombre}, Categoría: {Categoria}, Valor: ${Valor:F2}, Cantidad: {Cantidad}, P.Unitario: ${PrecioUnitario:F2}";
        }
    }
}
