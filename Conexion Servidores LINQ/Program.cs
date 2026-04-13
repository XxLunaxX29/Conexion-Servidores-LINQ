using Conexion_Servidores_LINQ;
using ConexionServidores;
using System.Data;

var xmlLoader = new XmlDatasetLoader();
var csvLoader = new CsvDatasetLoader();
var jsonLoader = new JsonDatasetLoader();
var datasetManager = new DatasetManager("MiDataset");
var dataExtractor = new DataExtractor();
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
    Console.WriteLine("10. Extraer datos primordiales");
    Console.WriteLine("11. Migrar datos a SQL Server");
    Console.WriteLine("12. Migrar datos a MariaDB");
    Console.WriteLine("13. Migrar datos a PostgreSQL");
    Console.WriteLine("14. Consultar datos desde SQL Server");
    Console.WriteLine("15. Consultar datos desde MariaDB");
    Console.WriteLine("16. Consultar datos desde PostgreSQL");
    Console.WriteLine("17. Ver columnas disponibles");
    Console.WriteLine("18. Salir");
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
                ExtraerDatosPrimordiales(dataTable, dataExtractor);
            break;

        case "11":
            if (ValidarDatos(archivosCargados))
                MigrarASQL(dataTable, dataExtractor);
            break;

        case "12":
            if (ValidarDatos(archivosCargados))
                MigrarAMariaDB(dataTable, dataExtractor);
            break;

        case "13":
            if (ValidarDatos(archivosCargados))
                MigrarAPostgreSQL(dataTable, dataExtractor);
            break;

        case "14":
            ConsultarDesdeSQL();
            break;

        case "15":
            ConsultarDesdeMariaDB();
            break;

        case "16":
            ConsultarDesdePostgreSQL();
            break;

        case "17":
            if (ValidarDatos(archivosCargados))
                MostrarColumnasDisponibles(dataTable);
            break;

        case "18":
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
    int anchoDisponible = ventanaAncho / numColumnas;
    return Math.Min(anchoDisponible, 20);
}

static void MostrarTabla(DataTable dataTable, int filaInicio, int filaFin, int anchoColumna)
{
    // Mostrar encabezados
    foreach (DataColumn column in dataTable.Columns)
    {
        string nombre = column.ColumnName.Length > anchoColumna
            ? column.ColumnName.Substring(0, anchoColumna - 2) + ".."
            : column.ColumnName;
        Console.Write(nombre.PadRight(anchoColumna) + "| ");
    }
    Console.WriteLine();
    Console.WriteLine(new string('-', Console.WindowWidth - 1));

    // Mostrar datos
    for (int i = filaInicio; i < filaFin; i++)
    {
        foreach (var cell in dataTable.Rows[i].ItemArray)
        {
            string valor = cell.ToString();
            if (valor.Length > anchoColumna)
                valor = valor.Substring(0, anchoColumna - 2) + "..";
            Console.Write(valor.PadRight(anchoColumna) + "| ");
        }
        Console.WriteLine();
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

static void ExtraerDatosPrimordiales(DataTable dataTable, DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════════════════╗");
    Console.WriteLine("║     EXTRAER DATOS PRIMORDIALES (ID, NOMBRE, ETC)  ║");
    Console.WriteLine("╚════════════════════════════════════════════════════╝\n");

    extractor.ConfigurarConDataTable(dataTable);

    while (true)
    {
        Console.Clear();
        Console.WriteLine("╔════════════════════════════════════════════════════╗");
        Console.WriteLine("║              OPCIONES DE EXTRACCIÓN                ║");
        Console.WriteLine("╚════════════════════════════════════════════════════╝\n");

        var mapeo = extractor.ObtenerMapeo();

        if (mapeo.Count > 0)
        {
            Console.WriteLine("Campos detectados:");
            foreach (var m in mapeo)
            {
                Console.WriteLine($"  ✓ {m.Key.ToUpper()} → '{m.Value}'");
            }
            Console.WriteLine();
        }

        Console.WriteLine("Opciones:");
        Console.WriteLine("1. Mostrar datos primordiales");
        Console.WriteLine("2. Reasignar campos automáticamente");
        Console.WriteLine("3. Mapear campo personalizado");
        Console.WriteLine("4. Exportar datos primordiales a CSV");
        Console.WriteLine("5. Volver al menú principal");
        Console.Write("\nOpción: ");

        string opcion = Console.ReadLine();

        switch (opcion)
        {
            case "1":
                var productos = extractor.ExtraerDatos();
                extractor.MostrarDatosPrimordiales(productos);
                Console.WriteLine("Presione cualquier tecla...");
                Console.ReadKey();
                break;

            case "2":
                extractor.LimpiarMapeo();
                extractor.ConfigurarConDataTable(dataTable);
                Console.WriteLine("\n✓ Campos reasignados. Presione cualquier tecla...");
                Console.ReadKey();
                break;

            case "3":
                MapearCampoPersonalizado(dataTable, extractor);
                break;

            case "4":
                ExportarDatosPrimordiales(extractor);
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

static void MapearCampoPersonalizado(DataTable dataTable, DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║      MAPEAR CAMPO PERSONALIZADO        ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.WriteLine("Campos estándar disponibles:");
    Console.WriteLine("1. id");
    Console.WriteLine("2. nombre");
    Console.WriteLine("3. categoria");
    Console.WriteLine("4. valor");
    Console.WriteLine("5. cantidad");
    Console.WriteLine("6. preciounitario");
    Console.Write("\nSeleccione campo (1-6): ");

    string[] campos = { "id", "nombre", "categoria", "valor", "cantidad", "preciounitario" };
    if (!int.TryParse(Console.ReadLine(), out int campoIndex) || campoIndex < 1 || campoIndex > 6)
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    string campoEstandar = campos[campoIndex - 1];

    Console.WriteLine("\nColumnas disponibles en el DataTable:");
    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        Console.WriteLine($"{i + 1}. {dataTable.Columns[i].ColumnName}");
    }

    Console.Write("\nSeleccione columna (número): ");
    if (!int.TryParse(Console.ReadLine(), out int columnaIndex) || columnaIndex < 1 || columnaIndex > dataTable.Columns.Count)
    {
        Console.WriteLine("✗ Opción no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    string nombreColumnaReal = dataTable.Columns[columnaIndex - 1].ColumnName;

    try
    {
        extractor.MapearColumnaPersonalizada(campoEstandar, nombreColumnaReal);
        Console.WriteLine("✓ Mapeo realizado correctamente. Presione cualquier tecla...");
        Console.ReadKey();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"✗ Error: {ex.Message}. Presione cualquier tecla...");
        Console.ReadKey();
    }
}

static void ExportarDatosPrimordiales(DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║    EXPORTAR DATOS PRIMORDIALES         ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la ruta del archivo CSV (ej: primordiales.csv): ");
    string rutaArchivo = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(rutaArchivo))
    {
        Console.WriteLine("✗ Ruta no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    try
    {
        var productos = extractor.ExtraerDatos();
        var dataTableExportacion = extractor.ExportarADataTable(productos);

        using (var writer = new StreamWriter(rutaArchivo, false, System.Text.Encoding.UTF8))
        {
            // Escribir encabezados
            var headers = dataTableExportacion.Columns.Cast<DataColumn>().Select(c => c.ColumnName);
            writer.WriteLine(string.Join(",", headers));

            // Escribir datos
            foreach (DataRow row in dataTableExportacion.Rows)
            {
                var values = row.ItemArray.Select(v => $"\"{v}\"");
                writer.WriteLine(string.Join(",", values));
            }
        }

        Console.WriteLine($"\n✓ Datos exportados correctamente a: {rutaArchivo}");
        Console.WriteLine($"  Total de registros: {productos.Count}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n✗ Error al exportar: {ex.Message}");
    }

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

static void MigrarASQL(DataTable dataTable, DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A SQL SERVER          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    extractor.ConfigurarConDataTable(dataTable);

    Console.Write("Ingrese la cadena de conexión SQL Server:\n(ej: Server=localhost;User Id=sa;Password=YourPassword): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorSQL(connectionString);

    Console.WriteLine("\n⏳ Creando base de datos y tabla...");
    var resultCrear = migrador.CrearTablaEnSQL().Result;
    if (!resultCrear.Success)
    {
        Console.WriteLine($"✗ Error: {resultCrear.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("✓ Base de datos lista");

    var productos = extractor.ExtraerDatos();
    Console.WriteLine($"✓ {productos.Count} productos extraídos\n");

    var resultMigracion = migrador.MigrarProductosAsync(productos).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MigrarAMariaDB(DataTable dataTable, DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A MARIADB             ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    extractor.ConfigurarConDataTable(dataTable);

    Console.Write("Ingrese la cadena de conexión MariaDB:\n(ej: Server=192.168.1.9;User Id=root;Password=Garo2006): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorMariaDB(connectionString);

    Console.WriteLine("\n⏳ Creando base de datos y tabla...");
    var resultCrear = migrador.CrearTablaEnMariaDB().Result;
    if (!resultCrear.Success)
    {
        Console.WriteLine($"✗ Error: {resultCrear.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("✓ Base de datos lista");

    var productos = extractor.ExtraerDatos();
    Console.WriteLine($"✓ {productos.Count} productos extraídos\n");

    var resultMigracion = migrador.MigrarProductosAsync(productos).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

static void MigrarAPostgreSQL(DataTable dataTable, DataExtractor extractor)
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║     MIGRAR DATOS A POSTGRESQL          ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    extractor.ConfigurarConDataTable(dataTable);

    // Mostrar columnas disponibles
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║      COLUMNAS EN EL ARCHIVO            ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    for (int i = 0; i < dataTable.Columns.Count; i++)
    {
        Console.WriteLine($"{i + 1}. {dataTable.Columns[i].ColumnName}");
    }

    // Mapeo automático
    Console.WriteLine("\n╔════════════════════════════════════════╗");
    Console.WriteLine("║        REALIZANDO MAPEO                ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    // Mapear automáticamente según similitud de nombres
    MapearColumnasAutomaticamente(dataTable, extractor);

    var mapeo = extractor.ObtenerMapeo();
    if (mapeo.Count == 0)
    {
        Console.WriteLine("⚠ No se detectaron campos automáticamente");
        Console.WriteLine("  Mapee manualmente en la opción 10 primero\n");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("✓ Campos mapeados:");
    foreach (var m in mapeo)
    {
        Console.WriteLine($"  {m.Key.ToUpper().PadRight(20)} → {m.Value}");
    }

    Console.WriteLine("\n");
    Console.Write("Ingrese la cadena de conexión PostgreSQL:\n(ej: Server=localhost;User Id=postgres;Password=YourPassword): ");
    string connectionString = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("✗ Conexión no válida. Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var migrador = new MigradorPostgreSQL(connectionString);

    Console.WriteLine("\n⏳ Creando base de datos y tabla...");
    var resultCrear = migrador.CrearTablaEnPostgreSQL().Result;
    if (!resultCrear.Success)
    {
        Console.WriteLine($"✗ Error: {resultCrear.Message}");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    Console.WriteLine("✓ Base de datos lista");

    var productos = extractor.ExtraerDatos();
    Console.WriteLine($"✓ {productos.Count} productos extraídos\n");

    if (productos.Count == 0)
    {
        Console.WriteLine("⚠ No hay productos extraídos. Verifique el mapeo.");
        Console.WriteLine("Presione cualquier tecla...");
        Console.ReadKey();
        return;
    }

    var resultMigracion = migrador.MigrarProductosAsync(productos).Result;

    Console.WriteLine($"\n{'=' * 50}");
    Console.WriteLine(resultMigracion.Message);
    Console.WriteLine($"{'=' * 50}");

    Console.WriteLine("\nPresione cualquier tecla...");
    Console.ReadKey();
}

// Nuevo método para mapear automáticamente
static void MapearColumnasAutomaticamente(DataTable dataTable, DataExtractor extractor)
{
    var columnNames = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName.ToLower()).ToList();

    // Mapeos posibles según el formato de tu archivo
    var mapeos = new Dictionary<string, string>
    {
        { "id", "ID_Venta" },
        { "nombre", "Nombre_Producto" },
        { "categoria", "Categoria" },
        { "valor", "Total_Venta" },
        { "cantidad", "Cantidad" },
        { "preciounitario", "Precio_Unitario" }
    };

    foreach (var mapeo in mapeos)
    {
        // Buscar columna que coincida (case-insensitive)
        var columnaExistente = columnNames.FirstOrDefault(c =>
            c.Equals(mapeo.Value.ToLower()) ||
            c.Contains(mapeo.Value.ToLower().Replace("_", ""))
        );

        if (columnaExistente != null)
        {
            // Encontrar el nombre original con su case
            string columnaOriginal = dataTable.Columns.Cast<DataColumn>()
                .FirstOrDefault(c => c.ColumnName.ToLower() == columnaExistente)?.ColumnName;

            if (columnaOriginal != null)
            {
                try
                {
                    extractor.MapearColumnaPersonalizada(mapeo.Key, columnaOriginal);
                }
                catch
                {
                    // Continuar si hay error
                }
            }
        }
    }
}

static void ConsultarDesdeSQL()
{
    Console.Clear();
    Console.WriteLine("╔════════════════════════════════════════╗");
    Console.WriteLine("║  CONSULTAR DATOS DESDE SQL SERVER      ║");
    Console.WriteLine("╚════════════════════════════════════════╝\n");

    Console.Write("Ingrese la cadena de conexión SQL Server:\n(ej: Server=localhost;User Id=sa;Password=YourPassword): ");
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