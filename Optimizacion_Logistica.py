"""
Este script resuelve el Problema de Enrutamiento de Vehículos (CVRPTW)
cargando datos dinámicamente desde Google Sheets y calculando los tiempos
de viaje reales usando la API de Google Maps.

NUEVAS FUNCIONALIDADES:
- Lee los datos de despacho de una hoja "Datos".
- Lee la flota de camiones y sus tiempos de carga desde una hoja "Vehiculos".
- Aplica un factor de ralentización a los tiempos de viaje para simular la velocidad de un camión.
- Calcula salidas escalonadas de camiones basado en una hora de inicio y tiempos de carga.
- Ignora y reporta los camiones que no son necesarios para la ruta.
- Exporta una solución detallada a la hoja "Rutas", con una línea en blanco entre cada camión.

---------------------------------------------------------------------------
REQUISITOS DE INSTALACIÓN:
---------------------------------------------------------------------------
pip install ortools pandas gspread google-auth-oauthlib google-api-python-client googlemaps

---------------------------------------------------------------------------
CONFIGURACIÓN DE GOOGLE SHEETS:
---------------------------------------------------------------------------
Tu planilla debe tener dos hojas:
1.  **Hoja "Datos":**
    - Columnas: tienda, ubicacion, demanda, hora_apertura, hora_cierre, tiempo_servicio
    - La primera fila DEBE ser tu depósito/bodega.
2.  **Hoja "Vehiculos":**
    - Columnas: id_camion, capacidad_cajas, tiempo_carga_min
    - Cada fila representa un camión, su capacidad y su tiempo de carga individual.

---------------------------------------------------------------------------
CONFIGURACIÓN DEL SCRIPT:
---------------------------------------------------------------------------
1. Crea un archivo 'config.py' y añade tu clave: MAPS_API_KEY = "TU_CLAVE..."
2. Coloca tu archivo 'credentials.json' (de tipo Cuenta de Servicio) en esta carpeta.
3. Comparte tu Google Sheet con el 'client_email' de tu 'credentials.json' (rol 'Editor').

"""
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import googlemaps
from datetime import datetime
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# --- CONFIGURACIÓN DEL USUARIO ---
# Pega la URL completa de tu Google Sheet aquí
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ormMrQYe0G7aRcR_ppsLHiGrAlYut-59ULi_voK1GME"
# Define la hora en que empieza la carga de todos los camiones
HORA_INICIO_CARGA = "07:30"
# Factor para ajustar el tiempo de viaje de un auto a un camión (ej: 1.5 = 50% más lento)
FACTOR_RALENTIZACION_CAMION = 1.5


# Importa la clave de API desde el archivo de configuración
try:
    from config import MAPS_API_KEY
except ImportError:
    print("Error: No se encontró el archivo 'config.py' o la variable MAPS_API_KEY.")
    print("Por favor, crea el archivo 'config.py' con tu clave, ej: MAPS_API_KEY = 'AIzaSy...'")
    exit()

# --- FUNCIONES DE MANEJO DE DATOS ---

def time_to_minutes(time_str):
    """Convierte un string de tiempo 'HH:MM' a minutos desde la medianoche."""
    h, m = map(int, str(time_str).split(':'))
    return h * 60 + m

def load_all_data_from_sheets(sheet_url):
    """
    Se conecta a Google Sheets, lee "Datos" y "Vehiculos" y devuelve
    dos DataFrames y el objeto de la planilla.
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        client = gspread.authorize(creds)

        print("Accediendo a Google Sheet...")
        spreadsheet = client.open_by_url(sheet_url)

        # Cargar hoja "Datos"
        sheet_datos = spreadsheet.worksheet("Datos")
        data_loc = sheet_datos.get_all_records()
        df_locations = pd.DataFrame(data_loc)
        print("Datos cargados correctamente desde la hoja 'Datos'.")

        required_cols_loc = ['tienda', 'ubicacion', 'demanda', 'hora_apertura', 'hora_cierre', 'tiempo_servicio']
        if not all(col in df_locations.columns for col in required_cols_loc):
            print(f"Error: La hoja 'Datos' debe contener las columnas: {required_cols_loc}")
            return None, None, None

        # Cargar hoja "Vehiculos"
        sheet_vehiculos = spreadsheet.worksheet("Vehiculos")
        data_veh = sheet_vehiculos.get_all_records()
        df_vehicles = pd.DataFrame(data_veh)
        print("Datos cargados correctamente desde la hoja 'Vehiculos'.")

        required_cols_veh = ['id_camion', 'capacidad_cajas', 'tiempo_carga_min']
        if not all(col in df_vehicles.columns for col in required_cols_veh):
            print(f"Error: La hoja 'Vehiculos' debe contener las columnas: {required_cols_veh}")
            return None, None, None

        return df_locations, df_vehicles, spreadsheet

    except FileNotFoundError:
        print("Error: No se encontró el archivo 'credentials.json'.")
        return None, None, None
    except gspread.WorksheetNotFound as e:
        print(f"Error: No se encontró una hoja requerida en tu Google Sheet: {e}")
        return None, None, None
    except Exception as e:
        print(f"Ocurrió un error al acceder a Google Sheets: {e}")
        return None, None, None


def get_time_matrix(api_key, locations, slowdown_factor):
    """
    Usa la API de Google Distance Matrix para calcular el tiempo de viaje en minutos.
    Aplica un factor de ralentización para simular la velocidad de un camión.
    Maneja el límite de la API haciendo una llamada por cada origen.
    """
    try:
        gmaps = googlemaps.Client(key=api_key)
        print(f"\nObteniendo matriz de tiempos de viaje desde Google Maps API (factor camión: {slowdown_factor}x)...")
        time_matrix = []
        for i, origin in enumerate(locations):
            print(f"  Calculando tiempos desde el origen {i+1}/{len(locations)}: {origin}")
            response = gmaps.distance_matrix([origin], locations, mode="driving", departure_time=datetime.now())

            if response['rows'][0]['elements'][0]['status'] != 'OK':
                print(f"Error al obtener datos para el origen {origin}. Status: {response['rows'][0]['elements'][0]['status']}")
                return None

            row_elements = response['rows'][0]['elements']
            # Aplicar factor de ralentización
            time_row = [int((element['duration']['value'] / 60) * slowdown_factor) + 1 for element in row_elements]
            time_matrix.append(time_row)

        # Forzar a que el tiempo de viaje a sí mismo sea 0.
        for i in range(len(time_matrix)):
            time_matrix[i][i] = 0

        print("Matriz de tiempos obtenida con éxito.")
        return time_matrix

    except googlemaps.exceptions.ApiError as e:
        print(f"\nOcurrió un error con la API de Google Maps: {e}")
        return None
    except Exception as e:
        print(f"\nOcurrió un error inesperado al llamar a la API: {e}")
        return None

def create_data_model(df_locations, df_vehicles, time_matrix):
    """Prepara el diccionario de datos para el solver de OR-Tools."""
    data = {}
    data['time_matrix'] = time_matrix
    data['time_windows'] = [(time_to_minutes(row['hora_apertura']), time_to_minutes(row['hora_cierre'])) for _, row in df_locations.iterrows()]
    data['demands'] = df_locations['demanda'].tolist()
    data['service_times'] = df_locations['tiempo_servicio'].tolist()
    data['num_vehicles'] = len(df_vehicles)
    data['vehicle_capacities'] = df_vehicles['capacidad_cajas'].tolist()
    data['depot'] = 0
    return data

def process_solution(data, manager, routing, solution, time_dimension, store_names, vehicle_ids, start_loading_times):
    """Imprime la solución en la consola y la prepara para ser exportada."""
    print(f'\n--- Solución Óptima Encontrada ---')

    header = ['Camion ID', 'Secuencia', 'Tienda', 'Hora Inicio Carga', 'Hora Llegada', 'Hora Salida', 'Carga Acumulada (cajas)']
    solution_data_for_export = [header]
    vehicles_used = 0

    for vehicle_id in range(data['num_vehicles']):
        actual_vehicle_id = vehicle_ids[vehicle_id]
        index = routing.Start(vehicle_id)

        # Chequear si el vehículo se usa. Si la siguiente parada es el final, no se usa.
        if routing.IsEnd(solution.Value(routing.NextVar(index))):
            print(f"\nEl camión '{actual_vehicle_id}' no es necesario para esta ruta y no será despachado.")
            continue # Saltar al siguiente vehículo

        vehicles_used += 1
        plan_output = f'\nRuta para el camión {actual_vehicle_id} (Capacidad: {data["vehicle_capacities"][vehicle_id]} cajas):\n'

        # --- 1. Evento de Salida de Bodega ---
        sequence = 0
        node_index = manager.IndexToNode(index)
        store_name = store_names[node_index]

        start_load_val = start_loading_times[vehicle_id]
        departure_time_val = solution.Value(time_dimension.CumulVar(index))

        start_load_str = f"{start_load_val//60:02d}:{start_load_val%60:02d}"
        departure_str = f"{departure_time_val//60:02d}:{departure_time_val%60:02d}"

        plan_output += f"  {sequence}. Salida de '{store_name}' | Hora de Salida: {departure_str}\n"
        solution_data_for_export.append([actual_vehicle_id, sequence, store_name, start_load_str, "", departure_str, 0])

        # --- 2. Loop de Visitas a Tiendas ---
        route_load = 0
        while not routing.IsEnd(index):
            index = solution.Value(routing.NextVar(index))

            if routing.IsEnd(index):
                break

            sequence += 1
            node_index = manager.IndexToNode(index)
            store_name = store_names[node_index]
            route_load += data['demands'][node_index]

            arrival_time_val = solution.Value(time_dimension.CumulVar(index))
            departure_time_val = arrival_time_val + data['service_times'][node_index]

            arrival_str = f"{arrival_time_val//60:02d}:{arrival_time_val%60:02d}"
            departure_str = f"{departure_time_val//60:02d}:{departure_time_val%60:02d}"

            plan_output += (f"  {sequence}. Visita a '{store_name}'\n"
                           f"     Llegada: {arrival_str} | Salida: {departure_str} | Carga en camión: {route_load} cajas\n")
            solution_data_for_export.append([actual_vehicle_id, sequence, store_name, "", arrival_str, departure_str, route_load])

        # --- 3. Evento de Regreso a Bodega ---
        sequence += 1
        node_index = manager.IndexToNode(index)
        store_name = store_names[node_index]
        arrival_time_val = solution.Value(time_dimension.CumulVar(index))
        arrival_str = f"{arrival_time_val//60:02d}:{arrival_time_val%60:02d}"

        solution_data_for_export.append([actual_vehicle_id, sequence, store_name, "", arrival_str, "", route_load])
        plan_output += f"  {sequence}. Regreso a '{store_name}' | Llegada final: {arrival_str}\n"

        total_trip_time = solution.Value(time_dimension.CumulVar(index)) - solution.Value(time_dimension.CumulVar(routing.Start(vehicle_id)))
        plan_output += f'Carga total de la ruta: {route_load} cajas | Tiempo total en ruta: {total_trip_time} min\n'
        print(plan_output)

        # **NUEVO**: Añadir línea en blanco si hay más camiones por procesar
        if vehicles_used < sum(1 for v in range(data['num_vehicles']) if not routing.IsEnd(solution.Value(routing.NextVar(routing.Start(v))))):
             solution_data_for_export.append([""] * len(header))

    return solution_data_for_export


def export_solution_to_sheet(spreadsheet, data):
    """Crea/limpia la hoja "Rutas" y exporta los datos de la solución."""
    try:
        print("\nExportando solución a la hoja 'Rutas'...")
        # Si hay más que solo la cabecera, proceder a exportar.
        if len(data) > 1:
            try:
                worksheet = spreadsheet.worksheet("Rutas")
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="Rutas", rows=1, cols=20)

            worksheet.update('A1', data, value_input_option='USER_ENTERED')
            worksheet.format('A1:G1', {'textFormat': {'bold': True}})
            print("¡Solución exportada con éxito!")
        else:
            print("No se encontraron rutas activas para exportar.")

    except Exception as e:
        print(f"Ocurrió un error al exportar a Google Sheets: {e}")

def main():
    """Flujo principal: Carga, resuelve, muestra y exporta la solución."""
    df_locations, df_vehicles, spreadsheet = load_all_data_from_sheets(GOOGLE_SHEET_URL)
    if df_locations is None: return

    locations = df_locations['ubicacion'].tolist()
    store_names = df_locations['tienda'].tolist()
    vehicle_ids = df_vehicles['id_camion'].tolist()

    time_matrix = get_time_matrix(MAPS_API_KEY, locations, FACTOR_RALENTIZACION_CAMION)
    if time_matrix is None: return

    data = create_data_model(df_locations, df_vehicles, time_matrix)

    manager = pywrapcp.RoutingIndexManager(len(data['time_matrix']), data['num_vehicles'], data['depot'])
    routing = pywrapcp.RoutingModel(manager)

    def time_callback(from_index, to_index):
        from_node, to_node = manager.IndexToNode(from_index), manager.IndexToNode(to_index)
        return data['time_matrix'][from_node][to_node] + data['service_times'][from_node]

    transit_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    def demand_callback(from_index):
        return data['demands'][manager.IndexToNode(from_index)]

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_index, 0, data['vehicle_capacities'], True, 'Capacity')

    time = 'time'
    routing.AddDimension(transit_callback_index, 60, 1440, False, time)
    time_dimension = routing.GetDimensionOrDie(time)

    # --- LÓGICA DE SALIDAS ESCALONADAS ---
    print("\nCalculando salidas escalonadas de vehículos...")
    hora_inicio_min = time_to_minutes(HORA_INICIO_CARGA)
    tiempos_de_carga = df_vehicles['tiempo_carga_min'].tolist()
    start_loading_times = []
    current_time = hora_inicio_min

    for v_id in range(data['num_vehicles']):
        start_loading_times.append(current_time) # Guardar hora de inicio de carga
        departure_time = current_time + tiempos_de_carga[v_id] # Calcular hora de fin de carga (salida)

        index = routing.Start(v_id)
        # El camión no puede salir antes de que termine su carga
        time_dimension.CumulVar(index).SetMin(departure_time)
        print(f"  - Camión {vehicle_ids[v_id]} inicia carga a las {start_loading_times[-1]//60:02d}:{start_loading_times[-1]%60:02d} y puede salir a partir de las {departure_time//60:02d}:{departure_time%60:02d}")

        # El siguiente camión empieza a cargarse cuando el actual termina su carga.
        current_time = departure_time

    for loc_idx, time_win in enumerate(data['time_windows']):
        if loc_idx == data['depot']: continue
        index = manager.NodeToIndex(loc_idx)
        time_dimension.CumulVar(index).SetRange(time_win[0], time_win[1])

    for v_id in range(data['num_vehicles']):
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.End(v_id)))

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    search_parameters.local_search_metaheuristic = (routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.FromSeconds(10)

    solution = routing.SolveWithParameters(search_parameters)

    if solution:
        solution_for_export = process_solution(data, manager, routing, solution, time_dimension, store_names, vehicle_ids, start_loading_times)
        export_solution_to_sheet(spreadsheet, solution_for_export)
    else:
        print('\nNo se encontró una solución viable.')

if __name__ == '__main__':
    main()
