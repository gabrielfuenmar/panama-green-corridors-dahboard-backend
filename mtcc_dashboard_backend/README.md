# **MTCC Dashboard Backend**

# **Módulo de Funciones de Soporte AIS (`ais_support_func.py`)**

Este módulo proporciona funciones de soporte para el procesamiento de datos del Sistema de Identificación Automática (AIS) en entornos distribuidos usando Spark. Implementa métodos para gestionar versiones de especificaciones de buques y reconocer fases operativas basadas en datos geoespaciales.

---

### **1. `ais_specs_version_ret(mmsi_values, spark)`**

#### **Funcionalidad:**
Determina la versión de especificación del buque en función de su número IMO y la versión registrada en la base de datos de IHS.

#### **Parámetros:**
- `mmsi_values (Spark DataFrame)`: DataFrame de Spark que contiene información del buque, incluyendo los siguientes campos obligatorios:
  - `mmsi (int)`: Identificador MMSI del buque.
  - `vessel_type_main (string)`: Tipo principal del buque.
  - `length (float)`: Eslora del buque.
  - `width (float)`: Manga del buque.
  - `imo (int)`: Número IMO del buque.
  - `version (string)`: Versión registrada de especificación del buque.
- `spark (SparkSession)`: Sesión activa de Spark.

#### **Valor de retorno:**
- `ship_data (Spark DataFrame)`: DataFrame con la información del buque, incluyendo datos de IHS con versiones actualizadas y sin duplicados.

---

### **2. `op_phase_recog(dfspark, spark)`**

#### **Funcionalidad:**
Reconoce la fase operativa de un buque según la metodología del cuarto informe GHG de la IMO. Utiliza datos de velocidad, posición y carga para clasificar los buques en distintos estados operativos.

#### **Parámetros:**
- `dfspark (Spark DataFrame)`: DataFrame de Spark que contiene información AIS del buque, incluyendo:
  - `latitude (float)`: Latitud del buque.
  - `longitude (float)`: Longitud del buque.
  - `sog (float)`: Velocidad sobre el fondo (SOG).
  - `load (float)`: Carga del buque.
  - `StandardVesselType (string)`: Tipo estándar del buque.
- `spark (SparkSession)`: Sesión activa de Spark.

#### **Valor de retorno:**
- `dfspark (Spark DataFrame)`: DataFrame con una columna adicional `op_phase`, que categoriza la fase operativa del buque en una de las siguientes:
  - **Anchorage**
  - **Berth**
  - **Slow transit**
  - **Normal cruising**
  - **Manoeuvring**

#### **Flujo interno del proceso:**
1. **Carga de datos geoespaciales**
   - Se leen archivos de polígonos para determinar la proximidad a líneas costeras y puertos.

2. **Conversión de coordenadas**
   - Se utilizan claves H3 para la agrupación geoespacial.

3. **Clasificación de fase operativa**
   - Se aplican reglas basadas en la velocidad y proximidad al puerto para determinar la fase operativa.

4. **Manejo de excepciones**
   - Se validan errores y se gestionan posibles datos faltantes.

---

## **Procesamiento de Datos AIS para Puertos de Panamá desde UNGP (`panama_ports_statistics.py`)**

Este módulo activa el método de generación de estadísticas para visitas portuarias en Panamá. La versión actual solo incluye puertos cercanos al Canal de Panamá.

---

#### **Parámetros:**
- **Variables de entorno**:
  - `SPOT_TOKEN`: Token para la API de Spot Ocean.
  - `S3_ACCESS_KEY_MTCC`: Clave de acceso AWS S3.
  - `S3_SECRET_KEY_MTCC`: Clave secreta AWS S3.
- **Entradas de datos AIS**:
  - Registros de posición AIS de buques recuperados desde AWS S3 dentro de un rango de fechas específico.
  - Especificaciones de buques a partir de versiones IHS.
- **Entradas geoespaciales**:
  - Datos de polígonos EEZ (Zona Económica Exclusiva) para Panamá.
  - Índices H3 de alta resolución para el Canal de Panamá.

#### **Valor de retorno:**
- `ais_sample_ports (DataFrame)`: DataFrame PySpark que contiene llamadas a puerto, mmsi, hora de atraque (`berthing`) y hora de anclaje (`anchoring`) antes del atraque.

#### **Flujo Interno del Proceso:**
1. **Cargar datos geoespaciales**:
   - Recuperar los polígonos EEZ de Panamá y los límites del Canal de Panamá.
   - Extraer índices H3 únicos para filtrado espacial.

2. **Inicializar sesión Spark en Spot Ocean**:
   - Autenticarse utilizando `SPOT_TOKEN` y establecer una sesión Spark.

3. **Recuperar datos AIS**:
   - Conectar a AWS S3 y extraer registros AIS para el rango de fechas especificado.
   - Filtrar los datos AIS utilizando los índices H3 del Canal de Panamá.

4. **Unir especificaciones de buques**:
   - Recuperar versiones de buques desde IHS y emparejarlas con los registros AIS.
   - Extraer dimensiones de los buques (`length`, `width`, `vessel_type_main`).

5. **Detectar paradas en puerto**:
   - Aplicar `port_stops_cutoff` para segmentar paradas y transiciones de buques.
   - Filtrar valores MMSI relevantes para buques que hicieron paradas en puerto.

6. **Mejorar especificaciones de buques**:
   - Combinar especificaciones de buques con MMSI relacionados con puertos.
   - Aplicar reglas de adaptación del IMO GHG4 para cumplimiento de emisiones.

7. **Detectar fases operativas**:
   - Clasificar operaciones del buque (`Berth`, `Anchorage`, etc.) utilizando `op_phase_recog`.

8. **Unir segmentos de parada**:
   - Unir segmentos consecutivos de `Anchorage` protegiendo las fases `Berth`.
   - Unir segmentos consecutivos de `Berth` protegiendo las fases `Anchorage`.

9. **Recuperar registros de Berth y Anchorage**:
   - Extraer los segmentos `Berth` y sus períodos anteriores de `Anchorage`.

10. **Extraer el rango de fechas AIS**:
    - Calcular los timestamps más tempranos y más recientes en la muestra AIS.

11. **Guardar datos procesados (opcional)**:
    - Guardar resultados como archivo parquet para análisis posterior.

12. **Finalizar sesión Spark**:
    - Cerrar correctamente la sesión Spark para liberar recursos.

---

## **Módulo de Variables de Puertos de Panamá (`panama_ports_var.py`)**


Este módulo define listas clave relacionadas con remolcadores (tugboats), polígonos portuarios, áreas de anchorage y paradas del canal dentro del Canal de Panamá y sus alrededores. Estas variables ayudan en la clasificación y seguimiento de actividades marítimas.

---

### **1. `tugs`**

#### **Descripción:**
Una lista de remolcadores que operan en las zonas marítimas de Panamá.

#### **Valores:**
- BELEN, BOCAS DEL TORO, CACIQUE I, CALOVEBORA, CECIL HAYNES, CERRO ANCON, CERRO AZUL, CERRO CAMPANA,
- CERRO CANAJAGUA, CERRO GRANDE, CERRO ITAMUT, CERRO JEFE, CERRO LA VIEJA, CERRO MAJAGUAL, CERRO MAJAGULA, CERRO PANDO,
- CERRO PICACHO, CERRO PUNTA, CERRO SANTIAGO, CERRO TIGRE, CHANGUINOLA I, CHIRIQUI III, COCLE, COLON,
- DARIEN, DOLEGA, MCAULIFFE, ESTI, FARFAN, GILBERTO GUARDIA, GUIA, HERRERA, LIDER, LOS SANTOS, PACORA,
- PANAMA XIV, PARITA I, PEQUENI, RIO_BAYANO, RIO INDIO, RIO TUIRA, SAJALICES, TERIBE, TONOSI, UNIDAD, VERAGUAS I,
- GORGONA I, GUARDIA, CHANGUINOLA, SIXAOLA, CULEBRA.

---

### **2. `ports_pol`**

#### **Descripción:**
Una lista de puertos y terminales en Panamá.

#### **Valores:**
- Colon2000, Atlantic - PPC Cristobal, Pacific - PPC Balboa, Pacific - PATSA, Pacific - PSA, MIT, CCT, LNG terminal, Telfer, Oil Tanking.

---

### **3. `anch_pol`**

#### **Descripción:**
Una lista de áreas de anchorage en Panamá.

#### **Valores:**
- Atlantic Anchorage, Pacific Anchorage.

---

### **4. `stops`**

#### **Descripción:**
Una lista de paradas del canal en el Canal de Panamá.

#### **Valores:**
- Miraflores Waiting Area, Pedro Miguel Mooring Station, Gamboa Mooring Station, Gatun Anchorage.

---

# **Módulo de Procesamiento de Datos S3 (`s3_digest.py`)**

Este módulo proporciona funciones para interactuar con AWS S3 y recuperar datos AIS (Automatic Identification System). Incluye métodos para establecer una sesión S3, listar archivos disponibles y recuperar datos AIS según rangos de fechas y filtrado geoespacial.

---

### **1. `open_s3_session(p_access_key, p_secret_key, bucket_name="mtcclatam", folder_name="dash")`**

#### **Funcionalidad:**
Abre una sesión en AWS S3 y recupera una lista de archivos disponibles del bucket y carpeta especificados.

#### **Parámetros:**
- `p_access_key (string)`: Clave de acceso AWS.
- `p_secret_key (string)`: Clave secreta de acceso AWS.
- `bucket_name (string, opcional)`: Nombre del bucket en S3 (por defecto: `"mtcclatam"`).
- `folder_name (string, opcional)`: Ruta de la carpeta dentro del bucket (por defecto: `"dash"`).

#### **Valor de retorno:**
- `bucket_list (list)`: Lista de rutas de archivos disponibles en la carpeta especificada.

---

### **2. `ais_read_data_range(p_access_key, p_secret_key, spark, h3_int_list=[], dt_from=None, dt_to=None, folder="stops_all")`**

#### **Funcionalidad:**
Recupera datos AIS desde S3 para un rango de fechas especificado, y opcionalmente filtra usando índices geoespaciales H3.

#### **Parámetros:**
- `p_access_key (string)`: Clave de acceso AWS.
- `p_secret_key (string)`: Clave secreta de acceso AWS.
- `spark (SparkSession)`: Sesión activa de Spark.
- `h3_int_list (list, opcional)`: Lista de índices H3 para filtrar los datos AIS (por defecto: lista vacía).
- `dt_from (string, opcional)`: Fecha de inicio en formato `YYYY-MM-DD` (por defecto: última fecha disponible en S3).
- `dt_to (string, opcional)`: Fecha de fin en formato `YYYY-MM-DD` (por defecto: fecha de ayer).
- `folder (string, opcional)`: Carpeta que contiene archivos de datos AIS (por defecto: `"stops_all"`).

#### **Valor de retorno:**
- `ais_sample (Spark DataFrame)`: Datos AIS dentro del rango de fechas y restricciones espaciales especificadas.

#### **Flujo Interno del Proceso:**
1. **Abrir sesión S3 y recuperar lista de archivos.**
2. **Determinar rangos de fechas disponibles según los archivos en S3.**
3. **Validar y parsear el rango de fechas definido por el usuario.**
4. **Recuperar datos AIS desde la base de datos con filtrado H3 opcional.**
5. **Verificar posiciones AIS nuevas y retornar los datos procesados.**

---

# **Módulo de Reconocimiento de Paradas Portuarias (`port_stops_recognition.py`)**

Este módulo proporciona funciones para procesar datos AIS (Sistema de Identificación Automática) con el fin de analizar paradas portuarias y tránsitos por el canal. Ayuda a segmentar los movimientos de los buques e identificar puntos clave de transición dentro de puertos y áreas de anchorage en Panamá.

---

### **1. `port_stops_cutoff(ais_df)`**

#### **Funcionalidad:**
Filtra y procesa los datos AIS para identificar paradas de buques en puertos y áreas de anchorage.

#### **Parámetros:**
- `ais_df (Spark DataFrame)`: DataFrame de Spark que contiene registros de posición AIS de buques.

#### **Valor de retorno:**
- `ais_cuts (Spark DataFrame)`: DataFrame con paradas y transiciones de buques segmentadas en puertos y áreas de anchorage.

#### **Flujo Interno del Proceso:**
1. **Asignar números de fila para ordenar por MMSI.**
2. **Filtrar registros correspondientes a visitas a puertos y áreas de anchorage conocidas.**
3. **Calcular diferencias de tiempo y segmentar movimientos.**
4. **Identificar paradas extendidas (≥72 horas).**
5. **Detectar transiciones entre diferentes áreas portuarias.**
6. **Asegurar consistencia en la identificación de paradas.**
7. **Eliminar columnas innecesarias y retornar el DataFrame limpio.**

---

### **2. `merge_stop_segments(df_spark, reference_phase, protected_phase, time_limit='1H')`**

#### **Funcionalidad:**
Fusiona segmentos de paradas adyacentes en un DataFrame PySpark basándose en una fase de referencia, protegiendo otra fase.

#### **Parámetros:**
- `df_spark (DataFrame)`: DataFrame PySpark con registros de operación de buques.
- `reference_phase (str)`: Fase operativa usada como referencia para fusionar (por ejemplo, `'Anchorage'`).
- `protected_phase (str)`: Fase operativa que no debe fusionarse ni ser absorbida (por ejemplo, `'Berth'`).
- `time_limit (str)`: Límite máximo de tiempo permitido entre segmentos (por ejemplo, `'1H'` para una hora).

#### **Valor de retorno:**
- `df_spark (DataFrame)`: DataFrame PySpark actualizado con valores corregidos del segmento (`segr`).

#### **Flujo Interno del Proceso:**
1. **Convertir `time_limit` a segundos** para cálculos precisos de diferencia de tiempo.
2. **Definir una especificación de ventana** particionada por `flag` y `mmsi`, ordenada por `dt_pos_utc`.
3. **Extraer detalles del segmento anterior**:
   - Se recuperan `op_phase`, `segr` y `dt_pos_utc` previos para comparación.
4. **Calcular diferencia de tiempo** entre registros consecutivos.
5. **Fusionar segmentos consecutivos** si:
   - `op_phase` coincide con `reference_phase`.
   - El `op_phase` anterior también es `reference_phase`.
   - La diferencia de tiempo está dentro del `time_limit` especificado.
6. **Propagar asignaciones de segmento fusionado** para asegurar consistencia.
7. **Evitar la fusión de fases protegidas** restaurando identificadores originales de segmento para `protected_phase`.
8. **Limpiar columnas intermedias** y retornar el DataFrame procesado.

---

### **3. `get_berth_anchorage_records(df_spark)`**

#### **Funcionalidad:**
Recupera segmentos de `Berth` y sus segmentos de `Anchorage` precedentes, calculando además el valor más común de `Name` para cada grupo `flag` y `mmsi`.

#### **Parámetros:**
- `df_spark (Spark DataFrame)`: DataFrame PySpark que contiene registros de movimiento de buques.

#### **Valor de retorno:**
- `result_df (Spark DataFrame)`: DataFrame PySpark con detalles de `Berth` y `Anchorage` precedentes.

#### **Flujo Interno del Proceso:**
1. **Identificar el `Name` más frecuente** para cada grupo `flag` y `mmsi` cuando `op_phase` es `'Berth'`.
2. **Extraer timestamps del segmento `Berth`**:
   - Calcular el timestamp mínimo y máximo para cada segmento `Berth`.
3. **Encontrar segmentos de `Anchorage` precedentes**:
   - Identificar segmentos `Anchorage` que preceden a un segmento `Berth` usando el orden temporal.
   - Calcular el timestamp mínimo y máximo para esos segmentos de anchorage.
4. **Unir los datos de `Berth` y `Anchorage`**:
   - Realizar un left join para asociar cada segmento `Berth` con su `Anchorage` precedente, si existe.
5. **Retornar un DataFrame limpio** con `flag`, `mmsi`, `mode_name` y timestamps de `Berth` y `Anchorage`.

---

# **MTCC Dashboard Backend - Spot API Integration**

# **Módulo de la API de Spot (`spot_api.py`)**

Este módulo proporciona una interfaz para interactuar con el servicio Ocean Spark de Spot.io. Permite el envío de trabajos, monitoreo y eliminación de aplicaciones Spark en la infraestructura de Spot.io.

---

### **1. Clase `spot_connect`**

#### **Funcionalidad:**
Gestiona la autenticación y la interacción con la API Ocean Spark de Spot.io, permitiendo:
- Recuperar configuraciones de conexión Spark.
- Enviar trabajos Spark.
- Monitorear el estado de la aplicación.
- Esperar hasta que las aplicaciones estén en estado de ejecución.
- Eliminar aplicaciones.
- Crear una sesión Spark.

#### **Parámetros de inicialización:**
- `token (str)`: Token de autenticación para la API.

#### **Atributos de instancia:**
- `cluster_id (str)`: Identificador del clúster (oculto por razones de seguridad).
- `acct_id (str)`: Identificador de cuenta (oculto por razones de seguridad).
- `token (str)`: Token de autenticación para la API.
- `payload (dict)`: Carga útil JSON para el envío del trabajo.
- `app_id (str)`: ID de la aplicación asignado tras el envío.
- `app_url (str)`: URL para monitorear la aplicación Spark enviada.
- `config_name (str)`: Nombre de la configuración utilizada para el envío Spark.

---

### **2. `get_spark_connect_payload()`**

#### **Funcionalidad:**
Recupera la plantilla de configuración de Spark desde Spot.io y prepara la carga útil (`payload`) para el envío del trabajo.

#### **Valor de retorno:**
- `None` (actualiza `self.payload`).

---

### **3. `submit()`**

#### **Funcionalidad:**
Envía un trabajo Spark a Spot.io y recupera el ID de la aplicación asignado y la URL de monitoreo.

#### **Valor de retorno:**
- `response (requests.Response)`: Objeto de respuesta de la API que contiene los detalles del trabajo.

---

### **4. `appstate()`**

#### **Funcionalidad:**
Consulta el estado actual de la aplicación Spark enviada.

#### **Valor de retorno:**
- `status (str)`: Estado actual de la aplicación (por ejemplo, `"RUNNING"`, `"FAILED"`).

---

### **5. `wait_til_running(wait_seconds=10, max_wait_seconds=240)`**

#### **Funcionalidad:**
Consulta periódicamente el estado de la aplicación en intervalos definidos hasta que cambia de `"PENDING"` a un estado final o hasta alcanzar el tiempo máximo de espera.

#### **Parámetros:**
- `wait_seconds (int, opcional)`: Intervalo (en segundos) entre revisiones de estado (por defecto: 10).
- `max_wait_seconds (int, opcional)`: Tiempo máximo para esperar que la aplicación esté en ejecución (por defecto: 240).

#### **Valor de retorno:**
- `None`

---

### **6. `delete()`**

#### **Funcionalidad:**
Elimina la aplicación enviada desde el clúster Ocean Spark de Spot.io.

#### **Valor de retorno:**
- `response (requests.Response)`: Objeto de respuesta de la API que confirma la eliminación.

---

### **7. `spark_session()`**

#### **Funcionalidad:**
Crea y retorna una sesión Spark conectada al servicio Ocean Spark de Spot.io.

#### **Valor de retorno:**
- `spark (OceanSparkSession)`: Sesión Spark inicializada.

---



# **Módulo de Reconocimiento de Tránsitos** (`transits_recognition.py`)

Este módulo procesa datos AIS para identificar prospectos de tránsito y realiza análisis de clustering para detectar áreas de anchorage y paradas en puerto. Utiliza PySpark para el procesamiento distribuido de datos y el algoritmo DBSCAN de scikit-learn para las operaciones de agrupamiento.

---

## 1. Funciones Principales

### 1.1 `transits_prospects(ais_sample_pc, unique_id="imo")`

**Funcionalidad:**
Procesa una muestra de datos AIS para identificar prospectos de tránsito. La función segmenta los movimientos de los buques en grupos de tránsito basándose en ubicaciones de anchorage, brechas temporales y condiciones específicas de navegación. Filtra los registros utilizando un identificador único (por defecto es `"imo"`, pero cambia a `"mmsi"` si es necesario), ordena los datos por timestamp, calcula diferencias de tiempo entre registros consecutivos, y segmenta los datos en grupos según brechas prolongadas (≥72 horas) o cambios en los nombres de anchorage. Operaciones posteriores de agrupamiento y filtrado refinan los segmentos de tránsito validando accesos a puertos y paso por esclusas.

**Parámetros:**
- `ais_sample_pc` (PySpark DataFrame): DataFrame que contiene datos AIS.
- `unique_id` (str, opcional): Identificador del buque (por defecto `"imo"`). Si no hay valores de `"imo"`, se usa `"mmsi"`.

**Valor de retorno:**
Un DataFrame PySpark con los segmentos de tránsito identificados y sus atributos asociados.

---

### 1.2 `dbscan_pandas(data)`

**Funcionalidad:**
Realiza agrupamiento sobre datos de posición de buques utilizando el algoritmo DBSCAN para detectar áreas de anchorage y paradas en puertos. La función ajusta el parámetro epsilon según la dirección de viaje del buque—usando mayor sensibilidad para buques con rumbo sur. Agrupa puntos de datos según latitud y longitud, ayudando a identificar clusters significativos que indiquen paradas.

**Parámetros:**
- `data` (Pandas DataFrame): DataFrame que contiene datos de posición del buque con las siguientes columnas:
  - `latitude`
  - `longitude`
  - `dir` (dirección de viaje)

**Valor de retorno:**
Un DataFrame de Pandas con una columna adicional `"cluster"` que representa las asignaciones de agrupamiento detectadas.

---

### 1.3 `first_last(data)`

**Funcionalidad:**
Identifica los primeros y últimos índices válidos en un DataFrame o Serie de Pandas. Esta función auxiliar es útil para determinar los límites de una secuencia de datos de un buque.

**Parámetros:**
- `data` (Pandas DataFrame o Series): Secuencia de datos del buque.

**Valor de retorno:**
Una tupla que contiene el primer índice válido y el último índice válido.

---

# **Módulo de Emisiones GEI** (`emissions_ghg_imo`)

Este módulo calcula emisiones de gases de efecto invernadero (GEI) para buques utilizando datos AIS bajo la metodología IMO GHG4. Utiliza PySpark para el procesamiento distribuido de datos con el fin de estimar emisiones en función de la velocidad del buque, fase operativa, potencia del motor y factores de consumo de combustible.

---

## 1. Funciones Principales

### 1.1 `emissions_ghg_imo(df_spark_w_adapted_specs, spark)`

**Funcionalidad:**
Calcula emisiones de gases de efecto invernadero (GEI) para buques utilizando datos AIS siguiendo la metodología IMO GHG4. La función estima las emisiones mediante:
- Cálculo del factor de carga del buque a partir de la velocidad sobre el fondo y la velocidad máxima.
- Identificación de la fase operativa del buque (por ejemplo, Slow transit, Normal cruising, Berth, Manoeuvring, Anchorage).
- Cálculo de diferencias de tiempo entre registros AIS consecutivos para determinar el uso de energía.
- Estimación del consumo de potencia para motores principales, motores auxiliares y calderas auxiliares, con ajustes según la fase operativa.
- Carga de tablas auxiliares para consumo de combustible y factores de emisión.
- Cálculo del consumo de combustible y aplicación de factores de emisión para estimar emisiones de CO2, CH4 y N2O en gramos, que luego se convierten a toneladas.

**Parámetros:**
- `df_spark_w_adapted_specs` (PySpark DataFrame): DataFrame que contiene especificaciones del buque y datos de movimiento.
- `spark` (SparkSession): Sesión activa de Spark.

**Valor de retorno:**
Un DataFrame PySpark con las emisiones estimadas por buque, incluyendo:
- Potencia del motor principal y consumo de energía (por ejemplo, `me_pow`, `me_ene`)
- Valores de consumo de combustible para sistemas principal, auxiliar y caldera (por ejemplo, `me_con`, `ae_con`, `ab_con`, `pilot_con`)
- Estimaciones de emisiones en gramos para CO2 (`co2_g`), CH4 (`ch4_g`) y N2O (`n2o_g`)
- Estimaciones de emisiones convertidas a toneladas para CO2 (`co2_t`), CH4 (`ch4_t`) y N2O (`n2o_t`)

# **Módulo de Lanzador de Emisiones Locales** (`2_emissions_local_v1.py`)

Este módulo (ubicado en la carpeta `local_emissions_launcher`) orquesta los cálculos locales de emisiones GEI para datos AIS en la región de Panamá. Integra datos geoespaciales, recupera registros AIS desde AWS S3, los combina con especificaciones de buques, calcula las emisiones y guarda resultados agregados de vuelta en S3.

---

## 1. Pasos Principales

### 1.1 **Cargar Datos Geoespaciales**

**Funcionalidad:**
- Carga datos geográficos para la EEZ de Panamá (`land_eez_wkt.csv`) y el Canal de Panamá (`panama_canal.csv`) desde una carpeta especificada.
- Convierte los datos en GeoDataFrames, aplicando el sistema de referencia de coordenadas adecuado (CRS).
- Filtra para conservar solo la Zona Económica Exclusiva (EEZ) de Panamá.

**Parámetros:**
- *Ninguno* (las rutas de archivos se referencian internamente vía `ef.find_folder_csv`).

**Valor de retorno:**
- Sin retorno directo. Los datos se almacenan en GeoDataFrames (`land_eez` y `panama_canal`) para pasos posteriores.

---

### 1.2 **Generar Índices H3**

**Funcionalidad:**
- Itera sobre la geometría filtrada de la EEZ y genera índices H3 a resolución 5.
- Convierte los índices H3 basados en texto a enteros.
- Los agrega en listas (`h3_indeces_coast_int`) para usarlos luego en el filtrado de datos AIS.

**Parámetros:**
- *Ninguno* (la geometría proviene del GeoDataFrame `count_eez`).

**Valor de retorno:**
- Sin retorno directo. La lista `h3_indeces_coast_int` se utiliza para filtrar datos AIS por ubicación.

---

### 1.3 **Inicializar Sesión Spark**

**Funcionalidad:**
- Recupera la variable de entorno `SPOT_TOKEN`.
- Se conecta al clúster Spark de Spot Ocean usando `spot_api.spot_connect`.
- Envía un trabajo, espera hasta que esté en ejecución y finalmente obtiene una sesión Spark.

**Parámetros:**
- *Ninguno* (las credenciales se acceden vía variables de entorno).

**Valor de retorno:**
- Una sesión Spark (`spark`) para procesamiento distribuido de datos.

---

### 1.4 **Recuperar Credenciales de AWS**

**Funcionalidad:**
- Carga la clave de acceso AWS (`P_ACCESS_KEY`) y la clave secreta (`P_SECRET_KEY`) desde variables de entorno.

**Parámetros:**
- *Ninguno* (las claves se leen directamente desde variables de entorno).

**Valor de retorno:**
- Sin retorno directo. Las credenciales se almacenan en variables locales para operaciones S3.

---

### 1.5 **Verificar Archivos en S3 y Determinar Rango de Fechas**

**Funcionalidad:**
- Abre una sesión S3 (`open_s3_session`) y recupera la lista de archivos en el bucket.
- Identifica archivos de datos ya procesados (por ejemplo, `transits_pc_in_YYYY-MM-DD&YYYY-MM-DD.parquet`) para obtener rangos de fechas.
- Extrae fechas de inicio y fin desde los nombres de archivo y determina qué fechas aún necesitan procesamiento.
- Si todas las fechas ya están procesadas, el script finaliza.

**Parámetros:**
- *Ninguno* (el script revisa directamente nombres de archivo en S3).

**Valor de retorno:**
- Sin retorno directo. Los rangos de fechas a procesar se almacenan en variables locales (`range_to_do`, `fr_date`, `to_date`).

---

### 1.6 **Recuperar Datos AIS desde S3**

**Funcionalidad:**
- Usa `ais_read_data_range` para cargar datos AIS desde S3 dentro del rango de fechas especificado (`fr_date` a `to_date`).
- Aplica los índices H3 (`h3_indeces_coast_int`) para filtrado geoespacial.
- Si el conjunto de datos AIS está vacío (ya sea inicialmente o tras aplicar filtros), el script termina su ejecución.

**Parámetros:**
- `P_ACCESS_KEY` (str): Clave de acceso AWS.
- `P_SECRET_KEY` (str): Clave secreta AWS.
- `spark` (SparkSession): Sesión activa de Spark.
- `connect` (spot_connect object): Usado para gestionar el ciclo de vida del trabajo Spark.
- `h3_int_list` (list): Índices H3 para el filtrado (generados en el Paso 1.2).
- `dt_from` (str): Fecha de inicio.
- `dt_to` (str): Fecha de fin.
- `bypass_new_data_test` (bool): Determina si se deben omitir las verificaciones de nuevos datos.

**Valor de retorno:**
- Un DataFrame PySpark (`ais_sample`) que contiene los datos AIS.

---

### 1.7 **Recuperar Especificaciones de Buques y Unir con Datos AIS**

**Funcionalidad:**
- Carga versiones de especificaciones de buques (`get_ihs_versions`) y las une con los datos AIS.
- Determina la versión correcta de especificaciones del buque según el timestamp del registro AIS.
- Recupera detalles del buque (`imo`, `mmsi`, `vessel_type_main`, etc.) y los adapta al cumplimiento del estándar IMO GHG4.
- Une las especificaciones adaptadas al DataFrame AIS.

**Parámetros:**
- `spark` (SparkSession): Utilizado para lectura y unión de datos.
- `ais_sample` (DataFrame): Datos AIS del paso 1.6.

**Valor de retorno:**
- Un DataFrame PySpark actualizado (`ais_sample`) que incluye datos de especificaciones de buques.

---

### 1.8 **Calcular Emisiones Locales**

**Funcionalidad:**
- Llama a la función `emissions_ghg_imo` para calcular emisiones de CO2, CH4 y N2O para cada registro AIS.
- Aplica lógica de fase operativa, cálculos de potencia del motor y factores de emisión.

**Parámetros:**
- `ais_sample` (DataFrame): Datos AIS combinados con especificaciones del buque.
- `spark` (SparkSession): Para cálculos distribuidos.

**Valor de retorno:**
- Un DataFrame PySpark (`emissions_local`) con emisiones en gramos y toneladas.

---

### 1.9 **Agregar y Reestructurar Datos de Emisiones**

**Funcionalidad:**
- Extrae año, mes y resolución H3 desde el DataFrame de emisiones.
- Convierte las emisiones de formato ancho (`co2_t`, `ch4_t`, `n2o_t`) a formato largo utilizando la función `stack`.
- Agrega emisiones por año, mes, tipo de emisión, tipo de buque y resolución H3.

**Parámetros:**
- `emissions_local` (DataFrame): Datos de emisiones del paso 1.8.

**Valor de retorno:**
- Un DataFrame de Pandas (`em_pd`) que contiene las emisiones agregadas.

---

### 1.10 **Guardar Resultados en S3**

**Funcionalidad:**
- Lee cualquier archivo de emisiones existente (`emissions_local_panama_in`) desde S3 para fusionarlo con los nuevos datos.
- Actualiza los datos de emisiones combinadas agrupando y sumando según las dimensiones relevantes.
- Escribe los datos de emisiones actualizados y un archivo de log a S3, lo que permite que futuras ejecuciones omitan fechas ya procesadas.

**Parámetros:**
- `em_pd` (Pandas DataFrame): Datos de emisiones agregadas.
- `log_em` (Pandas DataFrame): Lista de fechas recientemente procesadas.

**Valor de retorno:**
- *Ninguno* (los archivos actualizados se guardan en S3).

---

### 1.11 **Detener Sesión Spark y Liberar Recursos**

**Funcionalidad:**
- Detiene la sesión de Spark.
- Elimina la aplicación de Spot Ocean Spark para liberar recursos.

**Parámetros:**
- `spark` (SparkSession): Sesión activa de Spark.
- `connect` (objeto spot_connect): Usado para eliminar la aplicación Spark.

**Valor de retorno:**
- *Ninguno* (los recursos son liberados y el script finaliza).

---

# **Módulo de Tránsitos del Canal de Panamá** (`1_transits_pc_v1.py`)

Este módulo (ubicado en la carpeta `transits_generator`) orquesta la identificación y procesamiento de los tránsitos de buques a través de la región del Canal de Panamá utilizando datos AIS. Integra datos geoespaciales, recupera y procesa registros AIS desde AWS S3, fusiona datos con especificaciones de buques, identifica eventos de tránsito y paradas en anchorage, y escribe los resultados de vuelta en S3.

---

## Pasos Principales

### 1. Cargar Datos Geoespaciales

**Funcionalidad:**
- Carga datos geográficos de la EEZ de Panamá (`land_eez_wkt.csv`) y del Canal de Panamá (`panama_canal.csv`).
- Convierte los datos en GeoDataFrames, configurando los sistemas de referencia de coordenadas (CRS) apropiados.
- Filtra los datos de la EEZ para conservar únicamente la Zona Económica Exclusiva de Panamá.

---

### 2. Generar Índices H3

**Funcionalidad:**
- Genera índices espaciales H3 para la EEZ (resolución 5) y el Canal de Panamá (resolución 10).
- Convierte los índices H3 de tipo cadena a enteros para un procesamiento eficiente.
- Crea listas de índices H3 para el filtrado espacial de los datos AIS.

---

### 3. Inicializar Sesión Spark

**Funcionalidad:**
- Se conecta al clúster Spark de Spot Ocean usando las credenciales de entorno (`SPOT_TOKEN`).
- Establece un entorno distribuido de procesamiento (sesión Spark).

---

### 4. Recuperar Credenciales de AWS

**Funcionalidad:**
- Carga las credenciales de acceso AWS (`P_ACCESS_KEY`, `P_SECRET_KEY`) desde variables de entorno.

---

### 5. Recuperar Datos AIS desde S3

**Funcionalidad:**
- Recupera datos AIS desde AWS S3 dentro de un rango de fechas especificado.
- Aplica filtrado espacial utilizando los índices H3 costeros.

---

### 6. Filtrar Datos AIS por Región del Canal y Tipo de Buque

**Funcionalidad:**
- Refina el conjunto de datos AIS para incluir solo registros relevantes dentro de la región del Canal de Panamá.
- Excluye remolcadores (`tug vessels`) y estandariza los nombres de áreas de anchorage.

---

### 7. Recuperar y Unir Especificaciones de Buques

**Funcionalidad:**
- Recupera especificaciones de buques compatibles con IMO basadas en los timestamps AIS.
- Integra los datos del buque en los registros AIS para un análisis enriquecido.

---

### 8. Identificar Prospectos de Tránsito

**Funcionalidad:**
- Identifica tránsitos potenciales agrupando registros AIS por MMSI e IMO.
- Elimina campos innecesarios para simplificar el manejo de los datos.

---

### 9. Identificar Paradas en Anchorage y Puertos

**Funcionalidad:**
- Determina paradas en anchorage analizando la velocidad de los buques dentro de polígonos específicos.
- Calcula timestamps de entrada y salida para paradas en anchorage y puerto.

---

### 10. Refinar Tránsitos del Canal

**Funcionalidad:**
- Calcula tiempos de entrada y salida de esclusas, direcciones de tránsito y calados más frecuentes.
- Identifica tránsitos directos y enriquece los registros de tránsito con datos de anchorage.

---

### 11. Guardar Datos de Tránsito y Paradas en S3

**Funcionalidad:**
- Escribe los datos de tránsitos refinados y datos de paradas por separado en AWS S3.
- Incluye metadatos de fecha para facilitar la recuperación de datos.

---

### 12. Finalizar Sesión Spark y Liberar Recursos

**Funcionalidad:**
- Detiene la sesión Spark.
- Libera recursos del entorno Spot Ocean.

---

## Panama Port Calls Module (`3_panama_port_calls.py`)

Este módulo procesa datos AIS dentro de la Zona Económica Exclusiva (ZEE) de Panamá para identificar escalas portuarias y eventos de fondeo. Aplica filtros espaciales mediante índices H3, recupera especificaciones de buques e identifica secuencias fondeo puerto para análisis.

---

### 1. Carga de ZEE y Puertos de Panamá

**Funcionalidad:**
Carga polígonos de la ZEE y ubicaciones portuarias, convirtiéndolos en GeoDataFrames para análisis espacial.

**Pasos internos:**
- Lectura de `land_eez_wkt.csv` y `ports_panama.csv`.
- Conversión a GeoDataFrames (EPSG:4326).
- Filtrado para quedarse solo con geometría de Panamá.

---

### 2. Generación de Índices H3 (Costa y Puertos)

**Funcionalidad:**
Convierte geometrías de la ZEE y puertos a índices H3 para filtrar espacialmente los mensajes AIS.

**Pasos internos:**
- Polígonos de la costa a resolución H3 nivel 5.
- Geometrías de puertos a resolución H3 nivel 10.
- Se obtienen celdas indexadas en formato entero.

---

### 3. Inicialización de Sesión Spark

**Funcionalidad:**
Conecta a una sesión Spark usando la API de Spot Ocean con un token.

**Parámetros:**
- `SPOT_TOKEN` desde variables de entorno.

---

### 4. Acceso a Archivos en S3

**Funcionalidad:**
Conecta a S3, lista archivos AIS disponibles y determina qué fechas faltan por procesar.

**Pasos internos:**
- Detecta archivos `transits_pc_in_*`.
- Compara con `port_stops_in_*` para excluir fechas ya procesadas.
- Extrae rangos de fechas válidos.

---

### 5. Recuperación de Datos AIS

**Funcionalidad:**
Obtiene mensajes AIS para las zonas H3 seleccionadas y fechas válidas, añadiendo versiones y especificaciones de buques.

**Pasos internos:**
- Filtrado por índice H3 costero.
- Unión con datos de puertos y versiones IHS.
- Enriquecimiento de registros MMSI con especificaciones GHG4.

---

### 6. Identificación de Eventos en Puerto y Fondeo

**Funcionalidad:**
Detecta escalas en puerto (SOG ≤ 1) y eventos de fondeo (1 < SOG ≤ 3) usando lógica basada en ventanas temporales.

**Pasos internos:**
- Eventos portuarios agrupados por nombre y separación de 24h.
- Fondeos agrupados por separación ≥ 1h.
- Solo se conservan escalas ≥ 1 hora.

---

### 7. Secuencias Fondeo → Puerto

**Funcionalidad:**
Detecta secuencias donde un fondeo es seguido de una escala portuaria en menos de 1 hora.

**Pasos internos:**
- Combinación de ambos tipos de eventos.
- Cálculo de diferencia temporal entre eventos.
- Enlace cuando la diferencia ≤ 3600s.

---

### 8. Guardado de Resultados

**Funcionalidad:**
Escribe los resultados a S3 bajo la clave `port_stops`, incluyendo especificaciones del buque.

**Output:**
Archivo parquet agrupado por `imo`, `mmsi`, duración y tipo de escala.
