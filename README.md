# **Panama Green Corridors Dahboard Backend**

Estas herramientas procesan datos AIS para estimación de emisiones, análisis operacional de buques y monitoreo del tráfico marítimo. Al transformar datos crudos en información estructurada, facilitan la integración en plataformas como la Plataforma Global de las Naciones Unidas (UNGP).

## **Tabla de Contenidos**
- [Uso](#uso)
- [Características](#características)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)

---

## **Uso**
Este backend proporciona funciones para el procesamiento de datos AIS, incluyendo clasificación de buques, análisis geoespacial e integración con AWS S3. A continuación, algunos casos de uso comunes:

---

### **1. Obtener Datos AIS desde S3**
```python
from s3_digest import ais_read_data_range

ais_data = ais_read_data_range("AWS_ACCESS_KEY", "AWS_SECRET_KEY", spark_session, dt_from="2025-01-01", dt_to="2025-01-15")
```

2. **Identificar la Fase Operacional del Buque**
```python
from ais_support_func import op_phase_recog

processed_data = op_phase_recog(ais_data, spark_session)
```

3. **Enviar un Job de Spark a Spot.io**
```python
from spot_api import spot_connect

spot = spot_connect(token="your_api_token")
spot.submit()
spot.wait_til_running()
spot.delete()

```

---

## **Características**

### **Procesamiento de Datos AIS**
- Recuperación de Especificaciones del Buque: Obtiene especificaciones del buque usando datos del IMO y de IHS.
- Reconocimiento de Fase Operacional: Clasifica actividades del buque según velocidad, posición y factores geoespaciales.

### **Manejo de Datos Geoespaciales**
- Monitoreo de Puertos en Panamá: Identifica la presencia de buques en puertos, zonas de fondeo y tránsito.
- Mapeo con Cuadrícula Hexagonal (H3): Utiliza índices H3 para análisis espacial.

### **Integración con AWS S3**
- Extracción y Filtrado de Datos: Obtiene datos AIS desde S3 con filtros geoespaciales opcionales.
- Consultas por Rango de Fechas: Permite consultas según un intervalo de fechas.

### **Gestión de Jobs en Spot.io**
- Envío de Jobs Spark: Automatiza la ejecución de trabajos Spark en el servicio Ocean Spark de Spot.io.
- Monitoreo y Finalización de Aplicaciones: Supervisa el estado de los jobs y gestiona su cierre.

---

Estructura del Proyecto
```
├── mtcc_dashboard_backend/
|   |--local_emissions_launcher/
|       |-- 2_emissions_local_v1.py
|   |--transits_generator/
|       |-- .gitkeep
|       |-- 1_transits_pc_v1.pt
│   ├── ais_support_func.py
|   |-- emissions_generator.py
│   ├── panama_ports_statistictis.py
│   ├── panama_ports_var.py
│   ├── port_stops_recognition.py
│   ├── s3_digest.py
│   ├── spot_api.py
|   |-- transits_recognition.py
│   ├── README.md
│
├── public/
│   ├── index.html
│   ├── style.css
│   ├── README.md
│
|-- README.md
└── .gitlab-ci.yml
```
---

Contribuciones

Colaboradores:
	•	Gabriel Moisés Fuentes
	•	Martín Gómez Torres

---

Licencia

Este proyecto está licenciado bajo [Licencia MIT](./LICENSE).

---
