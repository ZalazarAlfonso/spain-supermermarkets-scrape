# Next Steps: Automatización diaria de dbt

## Objetivo

Conseguir que una nueva carga diaria de productos termine automáticamente en las tablas silver y gold, sin ejecutar dbt manualmente desde el portátil.

Flujo objetivo:

```text
Cloud Scheduler
    ↓
Scrapers diarios / Cloud Run Jobs
    ↓
GCS
    ↓
Bronze BigQuery
    ↓
Readiness check
    ↓
dbt seed + dbt build
    ↓
Silver + Gold
```

## Estado actual

### Terminado

- Staging para Alcampo, Carrefour, DIA y Mercadona.
- Modelos intermediate, silver y gold.
- Estandarización de taxonomía sin productos completamente no mapeados.
- Parseo de tamaño de paquete/case size.
- Estandarización de marcas con alias y overrides revisados.
- Seeds de taxonomía, reglas, alias y correcciones por producto.
- Tests de seeds, silver, case size, marcas y gold.
- Build validado con dbt hasta gold.

### Pendiente

- Confirmar una carga Bronze diaria completa.
- Añadir un readiness check que bloquee dbt si falta un supermercado.
- Crear una imagen reproducible para ejecutar dbt.
- Crear un Cloud Run Job para el runner de dbt.
- Programar la ejecución diaria después de los scrapers.
- Añadir CI en GitHub Actions para Pull Requests.
- Añadir despliegue desde GitHub Actions.
- Añadir alertas de fallos y datos incompletos.
- Corregir la frescura de `fact_products_today`, que actualmente depende de la última fecha disponible.

Los dos primeros puntos ya tienen implementación local en `scripts/check_bronze_readiness.py` y `dbt/supermarket_dwh/Dockerfile`. Queda probarlos contra GCP y desplegarlos.

## Fase 1: Readiness check de Bronze

Implementado en `scripts/check_bronze_readiness.py`. Devuelve, por supermercado:

- Última fecha cargada.
- Número de filas.
- Presencia de la fecha objetivo.
- Estado `ready` o `not_ready`.

Debe fallar cuando:

- Falte uno de los cuatro supermercados.
- La fecha cargada sea anterior a la fecha objetivo.
- El número de filas sea cero o anormalmente bajo.

Criterio de aceptación:

```text
alcampo     date = D, rows > 0
carrefour   date = D, rows > 0
dia         date = D, rows > 0
mercadona   date = D, rows > 0
```

Prueba local (requiere ADC y acceso BigQuery):

```bash
python scripts/check_bronze_readiness.py \
  --project lab-spanish-smarkts-scraper \
  --target-date 2026-07-12
```

El proceso termina con código `0` únicamente cuando están listas las cuatro tablas.

## Fase 2: dbt runner reproducible

Implementado en `dbt/supermarket_dwh/Dockerfile` y `dbt/supermarket_dwh/entrypoint.sh` para ejecutar dbt desde un entorno limpio.

El runner debe:

1. Cargar credenciales mediante Workload Identity o secretos del entorno.
2. Ejecutar el readiness check antes de dbt.
3. Ejecutar `dbt seed --full-refresh`.
4. Ejecutar `dbt build --select silver_product_standardized+`.
5. Emitir un código de salida distinto de cero si falla un modelo o test.
6. Mostrar en logs el resumen final de dbt.

Comando principal previsto:

```bash
dbt seed --full-refresh
dbt build --select silver_product_standardized+
```

Prueba local desde la raíz del repositorio:

```bash
docker build -f dbt/supermarket_dwh/Dockerfile -t supermarket-dbt-runner .
docker run --rm \
  -e GOOGLE_CLOUD_PROJECT=lab-spanish-smarkts-scraper \
  -e DBT_PROJECT=lab-spanish-smarkts-scraper \
  -e DBT_LOCATION=europe-southwest1 \
  -e TARGET_DATE=2026-07-12 \
  supermarket-dbt-runner
```

Se puede cambiar el selector con `DBT_SELECT` y omitir una fecha explícita para usar la fecha del día.

## Fase 3: Cloud Run Job

Crear un Cloud Run Job para el runner de dbt.

Configuración necesaria:

- Proyecto GCP.
- Región BigQuery y Cloud Run.
- Cuenta de servicio.
- Permisos de BigQuery Job User.
- Permisos de escritura en los datasets silver y gold.
- Variables de entorno del perfil dbt.

El job debe poder ejecutarse manualmente antes de automatizarlo.

## Fase 4: Scheduling

Usar Cloud Scheduler para lanzar el job después de que terminen los scrapers.

Orden recomendado:

1. Terminan los cuatro scrapers.
2. GCS recibe los Parquet.
3. El loader actualiza Bronze.
4. El dbt runner comprueba readiness.
5. dbt reconstruye silver y gold.

Inicialmente puede usarse una hora fija con margen. Más adelante se puede reemplazar por un trigger dependiente de la finalización de las cargas.

## Fase 5: GitHub Actions

GitHub Actions debe cubrir CI y despliegue.

### Pull Requests

El workflow debe ejecutar:

```bash
dbt parse
dbt build --select state:modified+
```

Como primera versión, puede usar:

```bash
dbt build --select +fact_products_today
```

### Despliegue

El workflow de despliegue debe:

1. Autenticar GitHub con GCP mediante Workload Identity Federation.
2. Construir la imagen del dbt runner.
3. Publicarla en Artifact Registry.
4. Actualizar el Cloud Run Job.
5. Permitir un `workflow_dispatch` manual.

No se deben guardar claves JSON de cuentas de servicio en GitHub. Se deben usar credenciales federadas y secretos mínimos.

## Fase 6: Alertas y observabilidad

Añadir alertas para:

- Fallo de scraper.
- Fallo de carga Bronze.
- Falta de un supermercado.
- Cero filas.
- Fecha atrasada.
- Fallo de dbt.
- Incremento anormal de `Other`.
- Incremento anormal de productos sin marca.

El primer nivel puede ser Cloud Logging y notificación por correo. Después se puede añadir Slack u otro canal.

## Lo que puedo implementar aquí

Puedo implementar directamente en el repositorio:

- Readiness check de Bronze.
- Dockerfile del runner de dbt.
- Script de ejecución reproducible.
- Workflow de CI para Pull Requests.
- Workflow de despliegue.
- Configuración/documentación del Cloud Run Job.
- Consultas de calidad y alertas.
- Procedimiento de backfill y rerun.
- Tests locales y validación de dbt.

## Lo que tendrás que hacer tú

Estas acciones requieren acceso a tus cuentas y permisos externos:

- Crear o autorizar la cuenta de servicio GCP.
- Conceder permisos de BigQuery, Cloud Run y Artifact Registry.
- Configurar Workload Identity Federation entre GitHub y GCP.
- Crear los secretos o variables protegidas.
- Activar Cloud Scheduler/Eventarc si todavía no están activos.
- Confirmar la hora diaria de ejecución.
- Hacer la primera ejecución productiva y revisar los logs.

## Orden recomendado de trabajo

1. Implementar readiness check.
2. Crear y probar el dbt runner localmente.
3. Ejecutar el runner dentro de Cloud Run manualmente.
4. Añadir GitHub Actions de CI.
5. Añadir despliegue automático del job.
6. Activar Cloud Scheduler.
7. Añadir alertas.
8. Ejecutar un backfill controlado.

## Definición de terminado

La automatización estará lista cuando:

- Una carga diaria llegue a Bronze sin intervención manual.
- Readiness confirme los cuatro supermercados.
- Cloud Run ejecute dbt correctamente.
- Silver y gold tengan la fecha diaria esperada.
- Todos los tests críticos pasen.
- Un fallo sea visible sin revisar manualmente el portátil.
- Exista un comando o workflow para relanzar una fecha concreta.
