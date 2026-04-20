using Conexion_Servidores_LINQ;
using System.Data;

var xmlLoader = new XmlDatasetLoader();
var csvLoader = new CsvDatasetLoader();
var jsonLoader = new JsonDatasetLoader();
var datasetManager = new DatasetManager("MiDataset");
DataTable dataTable = null;
bool archivosCargados = false;
string tipoArchivoActual = "";
object loaderActual = null;

while (true)
{
    Console.Clear();
    Console.WriteLine("╔══════════════════════════╗");
    Console.WriteLine("║    GESTOR DE DATASETS    ║");
    Console.WriteLine("╚══════════════════════════╝\n");

    if (archivosCargados)
    {
        Console.WriteLine($"✓ Archivo cargado ({tipoArchivoActual}): {dataTable.Rows.Count} filas | {dataTable.Columns.Count} columnas");
        Console.WriteLine($"✓ Datos en memoria: {datasetManager.ObtenerTotal()} registros\n");
    }
    else
    {
        Console.WriteLine("✗ Sin archivo cargado\n");
    }

    Console.WriteLine("Seleccione una opción:");
    Console.WriteLine("1. Cargar archivo XML");
    Console.WriteLine("2. Cargar archivo CSV");
    Console.WriteLine("3. Cargar archivo JSON");
    Console.WriteLine("4. Mostrar datos");
    Console.WriteLine("5. Filtrar datos");
    Console.WriteLine("6. Buscar en memoria");
    Console.WriteLine("7. Agrupar datos");
    Console.WriteLine("8. Ver estadísticas");
    Console.WriteLine("9. Generar gráfico");
    Console.WriteLine("10. Migrar datos a SQL Server");
    Console.WriteLine("11. Migrar datos a MariaDB");
    Console.WriteLine("12. Migrar datos a PostgreSQL");
    Console.WriteLine("13. Consultar datos desde SQL Server");
    Console.WriteLine("14. Consultar datos desde MariaDB");
    Console.WriteLine("15. Consultar datos desde PostgreSQL");
    Console.WriteLine("16. Ver columnas disponibles");
    Console.WriteLine("17. Salir");
    Console.Write("\nOpción: ");

    string opcion = Console.ReadLine();

    switch (opcion)
    {
        case "1":
            CargarArchivoXml(ref xmlLoader, ref dataTable, ref archivosCargados, ref tipoArchivoActual, ref loaderActual, datasetManager);
            break;

        case "2":
            CargarArchivoCsv(ref csvLoader, ref dataTable, ref archivosCargados, ref tipoArchivoActual, ref loaderActual, datasetManager);
            break;

        case "3":
            CargarArchivoJson(ref jsonLoader, ref dataTable, ref archivosCargados, ref tipoArchivoActual, ref loaderActual, datasetManager);
            break;

        case "4":
            if (ValidarDatos(archivosCargados))
                MostrarDatos(dataTable);
            break;

        case "5":
            if (ValidarDatos(archivosCargados))
                FiltrarDatos(dataTable);
            break;

        case "6":
            if (ValidarDatos(archivosCargados))
                BuscarEnMemoria(datasetManager, dataTable);
            break;

        case "7":
            if (ValidarDatos(archivosCargados))
                AgruparDatos(datasetManager, dataTable);
            break;

        case "8":
            if (ValidarDatos(archivosCargados))
            {
                datasetManager.MostrarInformacion();
                Console.WriteLine("\nPresione cualquier tecla...");
                Console.ReadKey();
            }
            break;

        case "9":
            if (ValidarDatos(archivosCargados))
                GenerarGrafico(dataTable);
            break;

        case "10":
            if (ValidarDatos(archivosCargados))
                MigrarASQL(dataTable);
            break;

        case "11":
            if (ValidarDatos(archivosCargados))
                MigrarAMariaDB(dataTable);
            break;

        case "12":
            if (ValidarDatos(archivosCargados))
                MigrarAPostgreSQL(dataTable);
            break;

        case "13":
            ConsultarDesdeSQL();
            break;

        case "14":
            ConsultarDesdeMariaDB();
            break;

        case "15":
            ConsultarDesdePostgreSQL();
            break;

        case "16":
            if (ValidarDatos(archivosCargados))
                MostrarColumnasDisponibles(dataTable);
            break;

        case "17":
            Console.WriteLine("\n✓ Saliendo del programa...");
            return;

        default:
            Console.WriteLine("\n✗ Opción no válida. Presione cualquier tecla...");
            Console.ReadKey();
            break;
    }
}

// ==================== MÉTODOS ====================

static bool ValidarDatos(bool archivosCargados)
{
    if (!archivosCargados)
    {
        Console.WriteLine("\n✗ Debe cargar un archivo primero. Presione cualquier tecla...");
        Console.ReadKey();
        return false;
    }
    return true;
}

static int CalcularAnchoColumna(DataTable dataTable)
{
    int ventanaAncho = Console.WindowWidth - 4;
    int numColumnas = dataTable.Columns.Count;

    // Mínimo de 12 caracteres por columna, máximo de 25
    int anchoMinimo = 12;
    int anchoMaximo = 25;
    int anchoCalculado = ventanaAncho / numColumnas;

    // Ajustar al rango permitido
    return Math.Max(anchoMinimo, Math.Min(anchoMaximo, anchoCalculado));
}

static void MostrarTabla(DataTable dataTable, int filaInicio, int filaFin, int anchoColumna)
{
    if (dataTable.Columns.Count == 0 || filaInicio >= dataTable.Rows.Count)
        return;

    int ventanaAncho = Console.WindowWidth - 2;

    // Mostrar encabezados con ajuste automático
    var columnasVisibles = new List<int>();
    int anchoUsado = 0;

    // Determinar cuántas columnas caben en pantalla
    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        int anchoCol = Math.Min(15, Math.Max(10, dataTable.Columns[i].ColumnName.Length + 2));
        if (anchoUsado + anchoCol + 3 <= ventanaAncho)
        {
            columnasVisibles.Add(i);
            anchoUsado += anchoCol + 3; // +3 por el separador " | "
        }
        else if (columnasVisibles.Count == 0)
        {
            columnasVisibles.Add(i); // Al menos una columna
            break;
        }
        else
        {
            break;
        }
    }

    // Mostrar encabezados
    foreach (int colIdx in columnasVisibles)
    {
        string nombre = dataTable.Columns[colIdx].ColumnName;
        int ancho = Math.Min(15, Math.Max(10, nombre.Length + 2));

        if (nombre.Length > ancho - 2)
            nombre = nombre.Substring(0, ancho - 3) + "..";

        Console.Write(nombre.PadRight(ancho));
        Console.Write("| ");
    }
    Console.WriteLine();
    Console.WriteLine(new string('-', ventanaAncho));

    // Mostrar datos
    for (int i = filaInicio; i < filaFin && i < dataTable.Rows.Count; i++)
    {
        foreach (int colIdx in columnasVisibles)
        {
            string valor = dataTable.Rows[i][colIdx]?.ToString() ?? "";
            int ancho = Math.Min(15, Math.Max(10, dataTable.Columns[colIdx].ColumnName.Length + 2));

            if (valor.Length > ancho - 2)
                valor = valor.Substring(0, ancho - 3) + "..";

            Console.Write(valor.PadRight(ancho));
            Console.Write("| ");
        }
        Console.WriteLine();
    }

    // Mostrar información de navegación de columnas
    if (columnasVisibles.Count < dataTable.Columns.Count)
    {
        Console.WriteLine();
        Console.WriteLine($"⚠ Mostrando {columnasVisibles.Count} de {dataTable.Columns.Count} columnas");
        Console.WriteLine("┌─ COLUMNAS NO VISIBLES ─────────────────────────┐");

        int columnasNoVisibles = dataTable.Columns.Count - columnasVisibles.Count;
        int columnasAMostrar = Math.Min(8, columnasNoVisibles);

        for (int i = columnasVisibles.Count; i < columnasVisibles.Count + columnasAMostrar; i++)
        {
            string nombre = dataTable.Columns[i].ColumnName;
            if (nombre.Length > 45)
                nombre = nombre.Substring(0, 42) + "...";

            Console.WriteLine($"│ • {nombre.PadRight(43)} │");
        }

        if (columnasNoVisibles > columnasAMostrar)
            Console.WriteLine($"│ ... y {columnasNoVisibles - columnasAMostrar} columnas más                      │");

        Console.WriteLine("└────────────────────────────────────────────────┘");
    }
}

static void CargarArchivoXml(ref XmlDatasetLoader loader, ref DataTable dataTable, ref bool archivosCargados, ref string tipoArchivoActual, ref object loaderActual, DatasetManager datasetManager)
{
    Console.Clear();
    Console.WriteLine("╔═════════════════════════════════════════════╗");
    Console.WriteLine("║       CARGAR ARCHIVO XML                    ║");
    Console.WriteLine("╚═════════════════════════════════════════════╝\n");

    Console.Write("Ingrese la ruta del archivo XML (ej: datos.xml): ");
    string xmlPath = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(xmlPath))
    {
        Console.WriteLine("✗ Ruta no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.Write("Ingrese el nombre del elemento XML (ej: row): ");
    string elementName = Console.ReadLine() ?? "row";

    try
    {
        dataTable = loader.LoadXmlFile(xmlPath, elementName);
        datasetManager.CargarDesdeDataTable(dataTable);
        archivosCargados = true;
        tipoArchivoActual = "XML";
        loaderActual = loader;
        Console.WriteLine($"\n✓ Archivo cargado correctamente. Total de filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n✗ Error: {ex.Message}");
    }

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void CargarArchivoCsv(ref CsvDatasetLoader loader, ref DataTable dataTable, ref bool archivosCargados, ref string tipoArchivoActual, ref object loaderActual, DatasetManager datasetManager)
{
    Console.Clear();
    Console.WriteLine("╔═════════════════════════════════════════════╗");
    Console.WriteLine("║       CARGAR ARCHIVO CSV                    ║");
    Console.WriteLine("╚═════════════════════════════════════════════╝\n");

    Console.Write("Ingrese la ruta del archivo CSV (ej: datos.csv): ");
    string csvPath = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(csvPath))
    {
        Console.WriteLine("✗ Ruta no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.Write("¿El archivo tiene encabezados? (s/n): ");
    string tieneEncabezado = Console.ReadLine()?.ToLower() ?? "s";

    try
    {
        dataTable = loader.LoadCsvFile(csvPath, tieneEncabezado == "s");
        datasetManager.CargarDesdeDataTable(dataTable);
        archivosCargados = true;
        tipoArchivoActual = "CSV";
        loaderActual = loader;
        Console.WriteLine($"\n✓ Archivo cargado correctamente. Total de filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n✗ Error: {ex.Message}");
    }

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void CargarArchivoJson(ref JsonDatasetLoader loader, ref DataTable dataTable, ref bool archivosCargados, ref string tipoArchivoActual, ref object loaderActual, DatasetManager datasetManager)
{
    Console.Clear();
    Console.WriteLine("╔═════════════════════════════════════════════╗");
    Console.WriteLine("║       CARGAR ARCHIVO JSON                   ║");
    Console.WriteLine("╚═════════════════════════════════════════════╝\n");

    Console.Write("Ingrese la ruta del archivo JSON (ej: datos.json): ");
    string jsonPath = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(jsonPath))
    {
        Console.WriteLine("✗ Ruta no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    try
    {
        dataTable = loader.LoadJsonFile(jsonPath);
        datasetManager.CargarDesdeDataTable(dataTable);
        archivosCargados = true;
        tipoArchivoActual = "JSON";
        loaderActual = loader;
        Console.WriteLine($"\n✓ Archivo cargado correctamente. Total de filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n✗ Error: {ex.Message}");
    }

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void MostrarDatos(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔═════════════════════════════════════════════╗");
    Console.WriteLine("║         MOSTRAR DATOS CARGADOS              ║");
    Console.WriteLine("╚═════════════════════════════════════════════╝\n");

    if (dataTable.Rows.Count == 0)
    {
        Console.WriteLine("La tabla está vacía.");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    int anchoColumna = CalcularAnchoColumna(dataTable);
    int filasPorPagina = 10;
    int pagina = 0;
    int totalPaginas = (int)Math.Ceiling((double)dataTable.Rows.Count / filasPorPagina);

    while (true)
    {
        Console.Clear();
        Console.WriteLine("╔═════════════════════════════════════════════╗");
        Console.WriteLine($"║ PÁGINA {pagina + 1} de {totalPaginas} | Filas: {dataTable.Rows.Count} | Columnas: {dataTable.Columns.Count}   ║");
        Console.WriteLine("╚═════════════════════════════════════════════╝\n");

        int inicio = pagina * filasPorPagina;
        int fin = Math.Min(inicio + filasPorPagina, dataTable.Rows.Count);

        MostrarTabla(dataTable, inicio, fin, anchoColumna);

        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════╗");
        Console.WriteLine("║           OPCIONES DE NAVEGACIÓN       ║");
        Console.WriteLine("╠════════════════════════════════════════╣");
        Console.WriteLine("║  [A] - Página anterior                 ║");
        Console.WriteLine("║  [S] - Página siguiente                ║");
        Console.WriteLine("║  [V] - Volver al menú principal        ║");
        Console.WriteLine("╚════════════════════════════════════════╝");

        if (pagina == 0)
            Console.WriteLine("  (No hay página anterior)");

        if (pagina >= totalPaginas - 1)
            Console.WriteLine("  (No hay página siguiente)");

        Console.Write("\nIngrese una opción [A/S/V]: ");

        string nav = Console.ReadLine()?.ToUpper() ?? "";

        switch (nav)
        {
            case "A":
                if (pagina > 0)
                {
                    pagina--;
                }
                else
                {
                    Console.WriteLine("\n✗ No hay página anterior. Presione cualquier tecla...");
                    Console.ReadKey();
                }
                break;

            case "S":
                if (pagina < totalPaginas - 1)
                {
                    pagina++;
                }
                else
                {
                    Console.WriteLine("\n✗ No hay página siguiente. Presione cualquier tecla...");
                    Console.ReadKey();
                }
                break;

            case "V":
                return;

            default:
                Console.WriteLine("\n✗ Opción no válida. Ingrese [A], [S] o [V]. Presione cualquier tecla...");
                Console.ReadKey();
                break;
        }
    }
}

static void FiltrarDatos(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║           FILTRAR DATOS                ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    var filtros = new Dictionary<string, string>();
    bool agregarMasFiltros = true;

    while (agregarMasFiltros)
    {
        Console.WriteLine("\nColumnas disponibles:");
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            Console.WriteLine($"{i + 1}. {dataTable.Columns[i].ColumnName}");
        }

        Console.Write("\nSeleccione el número de columna (0 para terminar): ");
        if (!int.TryParse(Console.ReadLine(), out int columnaIndex))
        {
            Console.WriteLine("✗ Entrada no válida. Presione cualquier tecla...");
            Console.ReadKey();
            return;
        }

        if (columnaIndex == 0)
        {
            if (filtros.Count == 0)
            {
                Console.WriteLine("✗ Debe agregar al menos un filtro. Presione cualquier tecla...");
                Console.ReadKey();
                continue;
            }
            agregarMasFiltros = false;
            break;
        }

        if (columnaIndex < 1 || columnaIndex > dataTable.Columns.Count)
        {
            Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
            Console.ReadKey();
            continue;
        }

        string columna = dataTable.Columns[columnaIndex - 1].ColumnName;

        if (filtros.ContainsKey(columna))
        {
            Console.WriteLine($"\n⚠ Ya hay un filtro para '{columna}'. ¿Desea reemplazarlo? (s/n): ");
            string respuesta = Console.ReadLine()?.ToLower() ?? "n";
            if (respuesta != "s")
                continue;
        }

        Console.Write($"Ingrese el valor a buscar en '{columna}': ");
        string valor = Console.ReadLine();

        if (string.IsNullOrWhiteSpace(valor))
        {
            Console.WriteLine("✗ El valor no puede estar vacío. Presione cualquier tecla...");
            Console.ReadKey();
            continue;
        }

        filtros[columna] = valor;
        Console.WriteLine($"✓ Filtro agregado para '{columna}'");

        if (filtros.Count > 0)
        {
            Console.WriteLine($"\nFiltros actuales ({filtros.Count}):");
            foreach (var filtro in filtros)
            {
                Console.WriteLine($"  • {filtro.Key} = '{filtro.Value}'");
            }

            Console.Write("\n¿Desea agregar otro filtro? (s/n): ");
            string continuar = Console.ReadLine()?.ToLower() ?? "n";
            if (continuar != "s")
                agregarMasFiltros = false;
        }
    }

    var filasFiltradas = dataTable.AsEnumerable().ToList();

    foreach (var filtro in filtros)
    {
        filasFiltradas = filasFiltradas
            .Where(row => row[filtro.Key].ToString().Contains(filtro.Value, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    Console.Clear();
    Console.WriteLine($"╔════════════════════════════════════════╗");
    Console.WriteLine($"║  RESULTADOS: {filasFiltradas.Count} registros encontrados  ║");
    Console.WriteLine($"╚════════════════════════════════════════╝\n");

    if (filtros.Count > 0)
    {
        Console.WriteLine("Filtros aplicados:");
        foreach (var filtro in filtros)
        {
            Console.WriteLine($"  • {filtro.Key} contiene '{filtro.Value}'");
        }
        Console.WriteLine();
    }

    if (filasFiltradas.Count == 0)
    {
        Console.WriteLine("No se encontraron registros.");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    int anchoColumna = CalcularAnchoColumna(dataTable);

    foreach (DataColumn column in dataTable.Columns)
    {
        string nombre = column.ColumnName.Length > anchoColumna
            ? column.ColumnName.Substring(0, anchoColumna - 2) + ".."
            : column.ColumnName;
        Console.Write(nombre.PadRight(anchoColumna) + "| ");
    }
    Console.WriteLine();
    Console.WriteLine(new string('-', Console.WindowWidth - 1));

    foreach (var row in filasFiltradas)
    {
        foreach (var cell in row.ItemArray)
        {
            string valor = cell.ToString();
            if (valor.Length > anchoColumna)
                valor = valor.Substring(0, anchoColumna - 2) + "..";
            Console.Write(valor.PadRight(anchoColumna) + "| ");
        }
        Console.WriteLine();
    }

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void BuscarEnMemoria(DatasetManager datasetManager, DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║       BUSCAR EN MEMORIA (ÍNDICES)      ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("Columnas disponibles:");
    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        Console.WriteLine($"{i + 1}. {dataTable.Columns[i].ColumnName}");
    }

    Console.Write("\nSeleccione columna: ");
    if (!int.TryParse(Console.ReadLine(), out int columnaIndex) || columnaIndex < 1 || columnaIndex > dataTable.Columns.Count)
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    string columna = dataTable.Columns[columnaIndex - 1].ColumnName;
    Console.Write($"Ingrese valor a buscar en '{columna}': ");
    string valor = Console.ReadLine();

    Console.WriteLine("\n⏳ Buscando en índices...");
    var resultados = datasetManager.BuscarPorColumnaContiene(columna, valor);

    Console.Clear();
    Console.WriteLine($"╔════════════════════════════════════════╗");
    Console.WriteLine($"║  RESULTADOS: {resultados.Count} registros encontrados");
    Console.WriteLine($"╚════════════════════════════════════════╝\n");

    if (resultados.Count > 0)
    {
        var datosExportados = datasetManager.ExportarADataTable(resultados);
        int anchoColumna = CalcularAnchoColumna(datosExportados);
        MostrarTabla(datosExportados, 0, Math.Min(10, datosExportados.Rows.Count), anchoColumna);
    }
    else
    {
        Console.WriteLine("No se encontraron registros.");
    }

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void AgruparDatos(DatasetManager datasetManager, DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║       AGRUPAR DATOS POR COLUMNA        ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("Columnas disponibles:");
    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        Console.WriteLine($"{i + 1}. {dataTable.Columns[i].ColumnName}");
    }

    Console.Write("\nSeleccione columna para agrupar: ");
    if (!int.TryParse(Console.ReadLine(), out int columnaIndex) || columnaIndex < 1 || columnaIndex > dataTable.Columns.Count)
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    string columna = dataTable.Columns[columnaIndex - 1].ColumnName;
    Console.WriteLine($"\n⏳ Agrupando datos por '{columna}'...");

    var grupos = datasetManager.AgruparPor(columna);

    Console.Clear();
    Console.WriteLine($"╔════════════════════════════════════════╗");
    Console.WriteLine($"║  GRUPOS POR '{columna}': {grupos.Count} grupos");
    Console.WriteLine($"╚════════════════════════════════════════╝\n");

    int indice = 1;
    foreach (var grupo in grupos)
    {
        Console.WriteLine($"{indice}. {grupo.Key} → {grupo.Value.Count} registros");
        indice++;
    }

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void ConsultarDesdeSQL()
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║  CONSULTAR DATOS DESDE SQL SERVER      ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión SQL Server:\n(ej: Server=192.168.1.254:1433;Database=ConexionSQL;User Id=sa;Password=123): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var extractor = new ExtractorSQL(connectionString);

    while (true)
    {
        Console.Clear();
        Console.WriteLine("╔════════════════════════════════════════╗");
        Console.WriteLine("║        OPCIONES DE CONSULTA            ║");
        Console.WriteLine("╚════════════════════════════════════════╝\n");

        Console.WriteLine("1. Ver todos los productos");
        Console.WriteLine("2. Filtrar por categoría");
        Console.WriteLine("3. Filtrar por rango de precio");
        Console.WriteLine("4. Ver estadísticas");
        Console.WriteLine("5. Volver al menú principal");
        Console.Write("\nOpción: ");

        string opcion = Console.ReadLine();

        switch (opcion)
        {
            case "1":
                VerTodosLosProductos(extractor);
                break;
            case "2":
                FiltrarPorCategoria(extractor);
                break;
            case "3":
                FiltrarPorPrecio(extractor);
                break;
            case "4":
                VerEstadisticas(extractor);
                break;
            case "5":
                return;
            default:
                Console.WriteLine("\n✗ Opción no válida. Presione cualquier tecla...");
                Console.ReadKey();
                break;
        }
    }
}

static void VerTodosLosProductos(ExtractorSQL extractor)
{
    Console.WriteLine("\n⏳ Obteniendo productos de SQL Server...");
    var result = extractor.ObtenerProductosAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, "TODOS LOS PRODUCTOS");
}

static void FiltrarPorCategoria(ExtractorSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR CATEGORÍA               ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la categoría a buscar: ");
    string categoria = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(categoria))
    {
        Console.WriteLine("✗ Categoría no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorCategoriaAsync(categoria).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - CATEGORÍA: {categoria.ToUpper()}");
}

static void FiltrarPorPrecio(ExtractorSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR RANGO DE PRECIO         ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese el precio mínimo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMin) || precioMin < 0)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.Write("Ingrese el precio máximo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMax) || precioMax < precioMin)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorPrecioAsync(precioMin, precioMax).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - PRECIO: ${precioMin:F2} - ${precioMax:F2}");
}

static void VerEstadisticas(ExtractorSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     ESTADÍSTICAS DE PRODUCTOS          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("⏳ Obteniendo estadísticas...");
    var result = extractor.ObtenerEstadisticasAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n╔════════════════════════════════════════╗");
    Console.WriteLine("║        ESTADÍSTICAS GENERALES          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine($"Total de productos:        {result.Stats["Total"]}");
    Console.WriteLine($"Categorías únicas:         {result.Stats["CategoríasUnicas"]}");
    Console.WriteLine($"Precio mínimo:             ${result.Stats["PrecioMínimo"]:F2}");
    Console.WriteLine($"Precio máximo:             ${result.Stats["PrecioMáximo"]:F2}");
    Console.WriteLine($"Precio promedio:           ${result.Stats["PrecioPromedio"]:F2}");
    Console.WriteLine($"Cantidad total:            {result.Stats["CantidadTotal"]} unidades");
    Console.WriteLine($"Valor total:               ${result.Stats["ValorTotal"]:F2}");
    Console.WriteLine($"Cantidad promedio:         {result.Stats["CantidadPromedio"]:F2} unidades\n");

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void ConsultarDesdeMariaDB()
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║  CONSULTAR DATOS DESDE MARIADB         ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión MariaDB:\n(ej: Server=192.168.1.9;User Id=root;Password=Garo2006): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var extractor = new ExtractorMariaDB(connectionString);

    while (true)
    {
        Console.Clear();
        Console.WriteLine("╔════════════════════════════════════════╗");
        Console.WriteLine("║        OPCIONES DE CONSULTA            ║");
        Console.WriteLine("╚════════════════════════════════════════╝\n");

        Console.WriteLine("1. Ver todos los productos");
        Console.WriteLine("2. Filtrar por categoría");
        Console.WriteLine("3. Filtrar por rango de precio");
        Console.WriteLine("4. Ver estadísticas");
        Console.WriteLine("5. Volver al menú principal");
        Console.Write("\nOpción: ");

        string opcion = Console.ReadLine();

        switch (opcion)
        {
            case "1":
                VerTodosLosProductosMariaDB(extractor);
                break;
            case "2":
                FiltrarPorCategoriaMariaDB(extractor);
                break;
            case "3":
                FiltrarPorPrecioMariaDB(extractor);
                break;
            case "4":
                VerEstadisticasMariaDB(extractor);
                break;
            case "5":
                return;
            default:
                Console.WriteLine("\n✗ Opción no válida. Presione cualquier tecla...");
                Console.ReadKey();
                break;
        }
    }
}

static void VerTodosLosProductosMariaDB(ExtractorMariaDB extractor)
{
    Console.WriteLine("\n⏳ Obteniendo productos de MariaDB...");
    var result = extractor.ObtenerProductosAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, "TODOS LOS PRODUCTOS");
}

static void FiltrarPorCategoriaMariaDB(ExtractorMariaDB extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR CATEGORÍA               ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la categoría a buscar: ");
    string categoria = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(categoria))
    {
        Console.WriteLine("✗ Categoría no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorCategoriaAsync(categoria).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - CATEGORÍA: {categoria.ToUpper()}");
}

static void FiltrarPorPrecioMariaDB(ExtractorMariaDB extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR RANGO DE PRECIO         ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese el precio mínimo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMin) || precioMin < 0)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.Write("Ingrese el precio máximo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMax) || precioMax < precioMin)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorPrecioAsync(precioMin, precioMax).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - PRECIO: ${precioMin:F2} - ${precioMax:F2}");
}

static void VerEstadisticasMariaDB(ExtractorMariaDB extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╣");
    Console.WriteLine("║     ESTADÍSTICAS DE PRODUCTOS          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("⏳ Obteniendo estadísticas...");
    var result = extractor.ObtenerEstadisticasAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n╔════════════════════════════════════════╗");
    Console.WriteLine("║        ESTADÍSTICAS GENERALES          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine($"Total de productos:        {result.Stats["Total"]}");
    Console.WriteLine($"Categorías únicas:         {result.Stats["CategoriasUnicas"]}");
    Console.WriteLine($"Precio mínimo:             ${result.Stats["PrecioMinimo"]:F2}");
    Console.WriteLine($"Precio máximo:             ${result.Stats["PrecioMaximo"]:F2}");
    Console.WriteLine($"Precio promedio:           ${result.Stats["PrecioPromedio"]:F2}");
    Console.WriteLine($"Cantidad total:            {result.Stats["CantidadTotal"]} unidades");
    Console.WriteLine($"Valor total:               ${result.Stats["ValorTotal"]:F2}");
    Console.WriteLine($"Cantidad promedio:         {result.Stats["CantidadPromedio"]:F2} unidades\n");

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void ConsultarDesdePostgreSQL()
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║  CONSULTAR DATOS DESDE POSTGRESQL      ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión PostgreSQL:\n(ej: Server=localhost;User Id=postgres;Password=YourPassword): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var extractor = new ExtractorPostgreSQL(connectionString);

    while (true)
    {
        Console.Clear();
        Console.WriteLine("╔════════════════════════════════════════╗");
        Console.WriteLine("║        OPCIONES DE CONSULTA            ║");
        Console.WriteLine("╚════════════════════════════════════════╝\n");

        Console.WriteLine("1. Ver todos los productos");
        Console.WriteLine("2. Filtrar por categoría");
        Console.WriteLine("3. Filtrar por rango de precio");
        Console.WriteLine("4. Ver estadísticas");
        Console.WriteLine("5. Volver al menú principal");
        Console.Write("\nOpción: ");

        string opcion = Console.ReadLine();

        switch (opcion)
        {
            case "1":
                VerTodosLosProductosPostgreSQL(extractor);
                break;
            case "2":
                FiltrarPorCategoriaPostgreSQL(extractor);
                break;
            case "3":
                FiltrarPorPrecioPostgreSQL(extractor);
                break;
            case "4":
                VerEstadisticasPostgreSQL(extractor);
                break;
            case "5":
                return;
            default:
                Console.WriteLine("\n✗ Opción no válida. Presione cualquier tecla...");
                Console.ReadKey();
                break;
        }
    }
}

static void VerTodosLosProductosPostgreSQL(ExtractorPostgreSQL extractor)
{
    Console.WriteLine("\n⏳ Obteniendo productos de PostgreSQL...");
    var result = extractor.ObtenerProductosAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, "TODOS LOS PRODUCTOS");
}

static void FiltrarPorCategoriaPostgreSQL(ExtractorPostgreSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR CATEGORÍA               ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la categoría a buscar: ");
    string categoria = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(categoria))
    {
        Console.WriteLine("✗ Categoría no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorCategoriaAsync(categoria).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - CATEGORÍA: {categoria.ToUpper()}");
}

static void FiltrarPorPrecioPostgreSQL(ExtractorPostgreSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    FILTRAR POR RANGO DE PRECIO         ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese el precio mínimo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMin) || precioMin < 0)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.Write("Ingrese el precio máximo: $");
    if (!decimal.TryParse(Console.ReadLine(), out decimal precioMax) || precioMax < precioMin)
    {
        Console.WriteLine("✗ Precio no válido. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n⏳ Buscando productos...");
    var result = extractor.ObtenerProductosPorPrecioAsync(precioMin, precioMax).Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    extractor.MostrarTablaEnConsola(result.Data, $"PRODUCTOS - PRECIO: ${precioMin:F2} - ${precioMax:F2}");
}

static void VerEstadisticasPostgreSQL(ExtractorPostgreSQL extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     ESTADÍSTICAS DE PRODUCTOS          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("⏳ Obteniendo estadísticas...");
    var result = extractor.ObtenerEstadisticasAsync().Result;

    if (!result.Success)
    {
        Console.WriteLine($"\n✗ {result.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\n╔════════════════════════════════════════╗");
    Console.WriteLine("║        ESTADÍSTICAS GENERALES          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine($"Total de productos:        {result.Stats["Total"]}");
    Console.WriteLine($"Categorías únicas:         {result.Stats["CategoriasUnicas"]}");
    Console.WriteLine($"Precio mínimo:             ${result.Stats["PrecioMinimo"]:F2}");
    Console.WriteLine($"Precio máximo:             ${result.Stats["PrecioMaximo"]:F2}");
    Console.WriteLine($"Precio promedio:           ${result.Stats["PrecioPromedio"]:F2}");
    Console.WriteLine($"Cantidad total:            {result.Stats["CantidadTotal"]} unidades");
    Console.WriteLine($"Valor total:               ${result.Stats["ValorTotal"]:F2}");
    Console.WriteLine($"Cantidad promedio:         {result.Stats["CantidadPromedio"]:F2} unidades\n");

    Console.WriteLine("Presione cualquier tecla...");
    Console.ReadKey();
}

static void GenerarGrafico(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║         GENERAR GRÁFICO                ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    var columnasNumericas = new List<string>();
    var columnasParcialesNumericas = new Dictionary<string, double>();

    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        string nombreColumna = dataTable.Columns[i].ColumnName;
        int valoresNumericos = 0;
        int totalValores = 0;

        foreach (DataRow row in dataTable.Rows)
        {
            totalValores++;
            if (double.TryParse(row[i].ToString(), out _))
            {
                valoresNumericos++;
            }
        }

        double porcentajeNumerico = (double)valoresNumericos / totalValores * 100;

        if (valoresNumericos == totalValores)
        {
            columnasNumericas.Add(nombreColumna);
        }
        else if (porcentajeNumerico >= 80)
        {
            columnasParcialesNumericas[nombreColumna] = porcentajeNumerico;
        }
    }

    if (columnasNumericas.Count == 0 && columnasParcialesNumericas.Count == 0)
    {
        Console.WriteLine("✗ No hay columnas numéricas para graficar. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("Columnas numéricas disponibles:\n");
    int indice = 1;

    if (columnasNumericas.Count > 0)
    {
        Console.WriteLine("✓ Completamente numéricas:");
        foreach (var columna in columnasNumericas)
        {
            Console.WriteLine($"{indice}. {columna}");
            indice++;
        }
        Console.WriteLine();
    }

    if (columnasParcialesNumericas.Count > 0)
    {
        Console.WriteLine("⚠ Parcialmente numéricas:");
        foreach (var columna in columnasParcialesNumericas)
        {
            Console.WriteLine($"{indice}. {columna.Key} ({columna.Value:F1}% numéricos)");
            indice++;
        }
        Console.WriteLine();
    }

    Console.Write("Seleccione la columna a graficar (número): ");
    if (!int.TryParse(Console.ReadLine(), out int columnaIndex))
    {
        Console.WriteLine("✗ Entrada no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    string columnaSeleccionada = null;
    int contador = 1;

    foreach (var columna in columnasNumericas)
    {
        if (contador == columnaIndex)
        {
            columnaSeleccionada = columna;
            break;
        }
        contador++;
    }

    if (columnaSeleccionada == null)
    {
        foreach (var columna in columnasParcialesNumericas)
        {
            if (contador == columnaIndex)
            {
                columnaSeleccionada = columna.Key;
                break;
            }
            contador++;
        }
    }

    if (columnaSeleccionada == null)
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var valores = new List<double>();

    for (int i = 0; i < dataTable.Rows.Count; i++)
    {
        if (double.TryParse(dataTable.Rows[i][columnaSeleccionada].ToString(), out double valor))
        {
            valores.Add(valor);
        }
    }

    if (valores.Count == 0)
    {
        Console.WriteLine("✗ No se encontraron valores numéricos. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("\nTipos de gráficos disponibles:");
    Console.WriteLine("1. Valores individuales (serie de datos)");
    Console.WriteLine("2. Histograma de frecuencias (contar repeticiones)");
    Console.Write("\nSeleccione el tipo de gráfico (1 o 2): ");

    string tipoGrafico = Console.ReadLine();

    if (tipoGrafico == "1")
    {
        MostrarGraficoValoresIndividuales(columnaSeleccionada, valores, dataTable.Rows.Count);
    }
    else if (tipoGrafico == "2")
    {
        MostrarHistogramaFrecuencias(columnaSeleccionada, valores);
    }
    else
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
    }
}

static void MostrarGraficoValoresIndividuales(string columnaSeleccionada, List<double> valores, int totalFilas)
{
    if (valores.Count < totalFilas)
    {
        Console.WriteLine($"\n⚠ Nota: Se saltaron {totalFilas - valores.Count} filas con valores no numéricos.");
        Console.ReadKey();
    }

    Console.Clear();
    Console.WriteLine($"╔════════════════════════════════════════╗");
    Console.WriteLine($"║  GRÁFICO: {columnaSeleccionada}");
    Console.WriteLine($"║  Mostrando {Math.Min(15, valores.Count)} de {valores.Count} valores");
    Console.WriteLine($"╚════════════════════════════════════════╝\n");

    double maxValor = valores.Max();
    double minValor = valores.Min();

    for (int i = 0; i < Math.Min(15, valores.Count); i++)
    {
        double valor = valores[i];
        double porcentaje = (valor - minValor) / (maxValor - minValor);
        int barraLength = (int)(porcentaje * 50);

        if (barraLength == 0 && valor > minValor)
            barraLength = 1;

        Console.WriteLine($"{i + 1:D3}. {new string('█', barraLength)} {valor:F2}");
    }

    Console.WriteLine();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine($"║  Máximo: {maxValor:F2}");
    Console.WriteLine($"║  Mínimo: {minValor:F2}");
    Console.WriteLine($"║  Promedio: {valores.Average():F2}");
    Console.WriteLine($"║  Total de valores: {valores.Count}");
    Console.WriteLine("╚════════════════════════════════════════╝");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MostrarHistogramaFrecuencias(string columnaSeleccionada, List<double> valores)
{
    Console.Clear();
    Console.WriteLine($"╔════════════════════════════════════════╗");
    Console.WriteLine($"║  HISTOGRAMA DE FRECUENCIAS: {columnaSeleccionada}");
    Console.WriteLine($"╚════════════════════════════════════════╝\n");

    var frecuencias = new Dictionary<double, int>();

    foreach (var valor in valores)
    {
        if (frecuencias.ContainsKey(valor))
        {
            frecuencias[valor]++;
        }
        else
        {
            frecuencias[valor] = 1;
        }
    }

    var frecuenciasOrdenadas = frecuencias.OrderByDescending(x => x.Value).ThenBy(x => x.Key).ToList();

    int maxFrecuencia = frecuenciasOrdenadas.Max(x => x.Value);
    int limite = Math.Min(20, frecuenciasOrdenadas.Count);

    Console.WriteLine($"Mostrando {limite} de {frecuenciasOrdenadas.Count} valores únicos\n");

    for (int i = 0; i < limite; i++)
    {
        double valor = frecuenciasOrdenadas[i].Key;
        int frecuencia = frecuenciasOrdenadas[i].Value;
        double porcentaje = (double)frecuencia / maxFrecuencia;
        int barraLength = (int)(porcentaje * 50);

        if (barraLength == 0)
            barraLength = 1;

        Console.WriteLine($"{valor:F2} │ {new string('█', barraLength)} {frecuencia} veces ({(double)frecuencia / valores.Count * 100:F1}%)");
    }

    Console.WriteLine();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine($"║  Total de valores: {valores.Count}");
    Console.WriteLine($"║  Valores únicos: {frecuenciasOrdenadas.Count}");
    Console.WriteLine($"║  Valor más frecuente: {frecuenciasOrdenadas[0].Key:F2} ({frecuenciasOrdenadas[0].Value} veces)");
    Console.WriteLine($"║  Valor menos frecuente: {frecuenciasOrdenadas.Last().Key:F2} ({frecuenciasOrdenadas.Last().Value} veces)");
    Console.WriteLine($"║  Promedio: {valores.Average():F2}");
    Console.WriteLine($"║  Máximo: {valores.Max():F2}");
    Console.WriteLine($"║  Mínimo: {valores.Min():F2}");
    Console.WriteLine("╚════════════════════════════════════════╝");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MostrarColumnasDisponibles(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════════╗");
    Console.WriteLine("║        COLUMNAS DISPONIBLES EN LA TABLA    ║");
    Console.WriteLine("╚════════════════════════════════════════════╝\n");

    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        Console.WriteLine($"  {i + 1}. {dataTable.Columns[i].ColumnName}");
    }

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MigrarASQL(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A SQL SERVER          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión SQL Server:\n(ej: Server=localhost;User Id=sa;Password=YourPassword): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorSQL(connectionString);

    Console.WriteLine("\n⏳ Migrando datos a SQL Server...");
    var resultMigracion = migrador.MigrarDataTableAsync(dataTable).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MigrarAMariaDB(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A MARIADB             ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión MariaDB:\n(ej: Server=192.168.1.9;User Id=root;Password=Garo2006): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorMariaDB(connectionString);

    Console.WriteLine("\n⏳ Migrando datos a MariaDB...");
    var resultMigracion = migrador.MigrarDataTableAsync(dataTable).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MigrarAPostgreSQL(DataTable dataTable)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A POSTGRESQL          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión PostgreSQL:\n(ej: Server=localhost;User Id=postgres;Password=YourPassword): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorPostgreSQL(connectionString);

    Console.WriteLine("\n⏳ Migrando datos a PostgreSQL...");
    var resultMigracion = migrador.MigrarDataTableAsync(dataTable).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}