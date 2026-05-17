# Proyecto Apache Airflow - By Ansh Lamba

Este proyecto es un **programa educativo integral de Apache Airflow** que demuestra conceptos fundamentales y avanzados del orquestador de flujos de trabajo. Incluye ejemplos prácticos de DAGs, operadores, planificación, comunicación inter-tareas y orquestación.

## 📋 Descripción General

Apache Airflow es una plataforma de código abierto para crear, programar y monitorizar flujos de trabajo (workflows). Este proyecto contiene una colección de DAGs (Directed Acyclic Graphs) organizados progresivamente, desde conceptos básicos hasta patrones avanzados de orquestación.

### Características Principales

- ✅ Configuración Docker Compose para desarrollo local
- ✅ PostgreSQL como base de datos backend
- ✅ Redis como broker de mensajes
- ✅ CeleryExecutor para ejecución distribuida
- ✅ Ejemplos progresivos de DAGs y operadores
- ✅ Patrones de comunicación entre tareas (XCOMs)
- ✅ Ejecución paralela y ramificación
- ✅ Orquestación de DAGs (padre-hijo)
- ✅ Activos y dependencias basadas en assets

---

## 🗂️ Estructura del Proyecto

```
.
├── dags/                          # Directorio principal de DAGs
│   ├── 1_first_dag.py             # Concepto básico: tu primer DAG
│   ├── 2_dag_versioning.py        # Versionamiento de DAGs
│   ├── 3_operators.py             # Diferentes tipos de operadores
│   ├── 4_XCOMs_auto.py            # Comunicación automática entre tareas
│   ├── 5_XCOMs_kwargs.py          # Comunicación manual con XCOMs
│   ├── 6_parallel_tasks.py        # Ejecución paralela de tareas
│   ├── 7_branches.py              # Ramificación condicional
│   ├── 8_schedule_preset.py       # Programación con presets
│   ├── 9_schedule_cron.py         # Programación con expresiones cron
│   ├── 10_schedule_delta.py       # Programación con intervalos personalizados
│   ├── 11_incremental_load.py     # Carga incremental de datos
│   ├── 12_special_dates.py        # Manejo de fechas especiales
│   ├── 13_asset_dependent.py      # Dependencias basadas en activos
│   ├── asset.py                   # Definición de activos
│   ├── dag_orchestrate_1.py       # Primer DAG orquestado
│   ├── dag_orchestrate_2.py       # Segundo DAG orquestado
│   └── dag_orchestrate_parent.py  # DAG padre para orquestación
├── config/
│   └── airflow.cfg                # Configuración de Airflow
├── logs/                          # Registros de ejecución de DAGs
├── plugins/                       # Plugins personalizados de Airflow
├── docker-compose.yaml            # Configuración de servicios Docker
├── pyproject.toml                 # Dependencias del proyecto
├── main.py                        # Script principal
└── README.md                      # Esta documentación
```

---

## 📦 Requisitos

- **Python:** >= 3.12
- **Apache Airflow:** >= 3.2.1
- **Docker & Docker Compose:** Para ejecutar los servicios
- **Sistema Operativo:** Windows, macOS o Linux

### Dependencias del Proyecto

```toml
apache-airflow>=3.2.1
```

---

## 🚀 Instalación y Configuración

### 1. Clonar o preparar el proyecto

```bash
cd c:\Users\magal\Desktop\cursos-programación\airflow
```

### 2. Crear un ambiente virtual de Python

```powershell
# En Windows PowerShell
python -m venv .venv
(& .\.venv\Scripts\Activate.ps1)
```

### 3. Instalar dependencias

```bash
pip install -e .
```

### 4. Inicializar Airflow con Docker Compose

```bash
docker-compose up -d
```

Esto levantará los siguientes servicios:

- **PostgreSQL** (puerto 5432): Base de datos principal
- **Redis** (puerto 6379): Broker de mensajes
- **Airflow Webserver** (puerto 8080): Interfaz web
- **Airflow Scheduler**: Planificador de tareas
- **Airflow Worker**: Ejecutor de tareas (CeleryExecutor)

### 5. Acceder a la interfaz web

Abre tu navegador y ve a: **http://localhost:8080**

- **Usuario:** airflow
- **Contraseña:** airflow

---

## 🎓 Estructura de Aprendizaje: DAGs Incluidos

### Nivel 1: Fundamentos

#### 1️⃣ **1_first_dag.py** - Tu Primer DAG

- Concepto básico de un DAG
- Definición de tareas con `@task.python`
- Establecimiento de dependencias con `>>`

```python
@dag
def first_dag():
    @task.python
    def first_task():
        print("Hello World")

    @task.python
    def second_task():
        print("This is the second task")

    first_task() >> second_task()
```

#### 2️⃣ **2_dag_versioning.py** - Versionamiento de DAGs

- Cómo versionar tus DAGs
- Compatibilidad entre versiones

#### 3️⃣ **3_operators.py** - Operadores

- `@task.python`: Ejecutar código Python
- `@task.bash`: Ejecutar comandos Bash
- `BashOperator`: Operador Bash clásico
- Combinación de operadores

### Nivel 2: Comunicación Entre Tareas

#### 4️⃣ **4_XCOMs_auto.py** - Comunicación Automática

- Pasar datos entre tareas automáticamente con valores de retorno
- XCOMs implícitos

#### 5️⃣ **5_XCOMs_kwargs.py** - Comunicación Manual

- Uso manual de XCOMs con `ti.xcom_push()` y `ti.xcom_pull()`
- Control fino sobre qué datos se comparten

### Nivel 3: Ejecución Avanzada

#### 6️⃣ **6_parallel_tasks.py** - Tareas en Paralelo

- Ejecutar múltiples tareas simultáneamente
- Aprovechar la ejecución distribuida
- Procesar datos de múltiples fuentes

#### 7️⃣ **7_branches.py** - Ramificación Condicional

- Decisiones dentro de DAGs
- Ejecutar diferentes caminos basados en condiciones
- Flujos de control complejos

### Nivel 4: Planificación

#### 8️⃣ **8_schedule_preset.py** - Presets de Programación

- Usar presets predefinidos: `@daily`, `@hourly`, `@weekly`, etc.
- Configuración simple de cronogramas

#### 9️⃣ **9_schedule_cron.py** - Expresiones Cron

- Programación con formato Cron
- Ejemplos: `0 9 * * 1-5` (cada día laboral a las 9 AM)

#### 🔟 **10_schedule_delta.py** - Intervalos Personalizados

- Planificación con duraciones personalizadas
- Ejemplo: ejecutar cada 3 días

### Nivel 5: Patrones de Datos

#### 1️⃣1️⃣ **11_incremental_load.py** - Carga Incremental

- Cargar solo datos nuevos o modificados
- Optimizar el rendimiento en grandes volúmenes

#### 1️⃣2️⃣ **12_special_dates.py** - Fechas Especiales

- Manejo de fechas especiales y eventos
- Lógica condicional basada en calendarios

### Nivel 6: Assets y Orquestación

#### 1️⃣3️⃣ **13_asset_dependent.py** - Dependencias de Assets

- Usar Airflow Assets para dependencias de datos
- Trigger DAGs basados en disponibilidad de activos

#### 📦 **asset.py** - Definición de Activos

- Crear activos que pueden ser monitoreados
- Producir datos que otros DAGs pueden consumir

#### 🔗 **dag_orchestrate_1.py** y **dag_orchestrate_2.py** - DAGs Orquestados

- DAGs que pueden ser ejecutados por otros DAGs

#### 👨‍💼 **dag_orchestrate_parent.py** - DAG Padre

- Orquestar múltiples DAGs
- Usar `TriggerDagRunOperator` para ejecutar DAGs subordinados
- Esperar a que se completen para continuar

---

## 🏗️ Arquitectura de Infraestructura

### Servicios Docker

```yaml
PostgreSQL:
  - Puerto: 5432
  - Usuario: airflow
  - Contraseña: airflow
  - Base de datos: airflow
  - Propósito: Almacenar metadata de Airflow

Redis:
  - Puerto: 6379
  - Propósito: Broker de mensajes para CeleryExecutor

Airflow Webserver:
  - Puerto: 8080
  - Propósito: Interfaz web para monitoreo y gestión

Airflow Scheduler:
  - Propósito: Detectar y programar DAGs

Airflow Worker (Celery):
  - Propósito: Ejecutar tareas en paralelo
```

### Executor: CeleryExecutor

- Permite ejecución distribuida de tareas
- Escalable horizontalmente
- Ideal para cargas de trabajo complejas

---

## 📊 Conceptos Clave Explicados

### ¿Qué es un DAG?

Un **DAG** (Directed Acyclic Graph) es un conjunto de tareas organizadas con dependencias sin ciclos. Las tareas se ejecutan en el orden especificado por las dependencias.

### ¿Qué son las Tareas?

Las **tareas** son unidades de trabajo individuales. Pueden ser:

- **Operadores:** Unidades predefinidas (PythonOperator, BashOperator, etc.)
- **Sensores:** Esperan que ocurra un evento
- **Transferencias:** Mueven datos entre sistemas

### ¿Qué son los XCOMs?

Los **XCOMs** (Cross-Communications) permiten que las tareas se comuniquen pasándose datos entre sí.

### ¿Qué es la Orquestación?

La **orquestación** es controlar la ejecución de múltiples flujos de trabajo (DAGs) como una unidad coordinada.

---

## 🎯 Casos de Uso Comunes

| Caso de Uso                               | DAG de Ejemplo            |
| ----------------------------------------- | ------------------------- |
| Extracción y transformación de datos      | 6_parallel_tasks.py       |
| Carga incremental en data warehouse       | 11_incremental_load.py    |
| Procesamiento con reglas condicionales    | 7_branches.py             |
| Tareas programadas (ej: reportes diarios) | 8_schedule_preset.py      |
| Orquestación de múltiples pipelines       | dag_orchestrate_parent.py |
| Dependencias basadas en datos             | 13_asset_dependent.py     |

---

## 🔧 Operaciones Comunes

### Ver logs de un DAG

```bash
# Los logs se guardan en ./logs/
```

### Pausar/Reanudar un DAG

En la interfaz web (http://localhost:8080), usa el botón de alternancia junto al DAG.

### Ejecutar un DAG manualmente

En la interfaz web, haz clic en el botón "Trigger DAG".

### Limpiar datos

```bash
# Detener servicios
docker-compose down

# Eliminar volúmenes (datos persistentes)
docker-compose down -v
```

---

## 📝 Configuración

### Variables de Entorno Importantes

Edita el archivo `.env` (o `docker-compose.yaml`) para configurar:

```bash
AIRFLOW_IMAGE_NAME=apache/airflow:3.2.1
AIRFLOW_UID=50000
FERNET_KEY=<generar_clave_fernet>
AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt_secret
```

### Configuración de Airflow

El archivo `config/airflow.cfg` contiene la configuración principal de Airflow.

---

## 🐛 Troubleshooting

### Problema: DAGs no aparecen en la interfaz web

- Verifica que el archivo esté en `/dags/`
- Revisa los logs: `docker-compose logs scheduler`
- Asegúrate de que la sintaxis de Python sea correcta

### Problema: Tareas en estado "queued"

- Verifica que los workers estén corriendo: `docker-compose ps`
- Revisa que Redis y PostgreSQL estén activos

### Problema: Error de conexión a PostgreSQL

- Verifica que el contenedor de PostgreSQL esté corriendo
- Revisa las credenciales en docker-compose.yaml

---

## 📚 Recursos Adicionales

- [Documentación oficial de Apache Airflow](https://airflow.apache.org/)
- [Airflow Python SDK (v3+)](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Docker Compose para Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose.html)
- [Operadores disponibles](https://airflow.apache.org/docs/apache-airflow/stable/operators.html)

---

## 📖 Flujo de Aprendizaje Recomendado

1. **Semana 1:** DAGs básicos (archivos 1-3)
2. **Semana 2:** Comunicación entre tareas (archivos 4-5)
3. **Semana 3:** Ejecución avanzada (archivos 6-7)
4. **Semana 4:** Planificación (archivos 8-10)
5. **Semana 5:** Patrones de datos (archivos 11-12)
6. **Semana 6:** Orquestación (archivos 13 y orchestrate)

---

## 🤝 Contribuir

Para mejorar este proyecto educativo:

1. Prueba los ejemplos
2. Crea tus propios DAGs
3. Experimenta con diferentes operadores
4. Documenta lo que aprendas

---

## 📄 Licencia

Este proyecto sigue la licencia de Apache Airflow (Apache License 2.0).

---

## ✅ Checklist de Configuración Inicial

- [ ] Python 3.12+ instalado
- [ ] Repositorio clonado
- [ ] Ambiente virtual creado
- [ ] Dependencias instaladas (`pip install -e .`)
- [ ] Docker y Docker Compose instalados
- [ ] Docker Compose levantado (`docker-compose up -d`)
- [ ] Webserver accesible en http://localhost:8080
- [ ] DAGs visibles en la interfaz web

---

**Última actualización:** Mayo 2026

**Estado del Proyecto:** 🟢 Activo y en desarrollo continuo

¡Bienvenido a tu viaje de aprendizaje en Apache Airflow! 🚀
