<<<<<<< HEAD
# Análisis de ventas con Apache Spark

Proceso en **PySpark** que lee un archivo de ventas, separa los registros válidos de los inválidos según reglas de negocio y genera un resumen de ingresos por producto, por ciudad e indicadores generales.

El enunciado completo del ejercicio está en [`docs/exc-spark-1.md`](docs/exc-spark-1.md).

## Tecnologías

- Apache Spark 4.0.1 (PySpark), en modo local
- Python 3.12 + Java 17
- Docker y Docker Compose

## Estructura

```
Spark-Ejercicio/
├── data/input/sales.csv     # datos de entrada
├── docs/exc-spark-1.md      # enunciado del ejercicio
├── output/                  # resultados (se generan al ejecutar)
├── src/
│   ├── sales_analysis.py    # proceso principal
│   └── test_spark.py        # prueba mínima de que Spark arranca
├── Dockerfile               # imagen: Python + Java + PySpark
└── docker-compose.yml       # monta el proyecto en /app dentro del contenedor
```

## Cómo ejecutarlo

Requisito: Docker Desktop con el motor encendido (*Engine running*).

```bash
# 1. Construir la imagen (solo la primera vez o si cambia el Dockerfile)
docker compose build

# 2. Ejecutar el proceso
docker compose run --rm spark python src/sales_analysis.py
```

Los resultados quedan en la carpeta `output/` del proyecto, porque el `docker-compose.yml` monta la carpeta local dentro del contenedor. Esa carpeta no se versiona (está en `.gitignore`): se genera al ejecutar el proceso.

## Flujo del proceso

1. **Lectura**: se lee el CSV por encabezado, con todas las columnas como texto, y se verifica que existan las columnas requeridas. Si falta alguna, el proceso se detiene con un error claro.
2. **Limpieza**: se quitan espacios y los valores vacíos pasan a `null`.
3. **Conversión segura de tipos**: con `try_cast` y `try_to_timestamp`, un valor mal formado queda en `null` en lugar de detener la ejecución, y se registra como inválido.
4. **Validación**: cada registro recibe `is_valid` y `rejection_reason`. Si incumple varias reglas, aparecen todos los motivos.
5. **Ingreso**: `revenue = quantity * unit_price`, calculado solo para las ventas válidas.
6. **Resúmenes**: agrupaciones por producto y por ciudad.
7. **Indicadores y escritura**: se calculan los KPIs y se guardan los archivos de salida.

## Reglas de negocio aplicadas

| Regla | Motivo de rechazo |
|---|---|
| Todos los campos requeridos deben estar informados | `campo_vacio:<columna>` |
| `order_date` debe tener formato `YYYY-MM-DD` | `fecha_invalida` |
| `quantity` debe ser un número entero | `quantity_no_es_entero` |
| `unit_price` debe ser numérico | `unit_price_no_es_numero` |
| `quantity` debe ser mayor que cero | `quantity_menor_o_igual_a_cero` |
| `unit_price` debe ser mayor que cero | `unit_price_menor_o_igual_a_cero` |

Los registros inválidos no participan en ningún indicador de ventas.

## Archivos de salida

| Archivo | Contenido |
|---|---|
| `output/valid_sales.csv` | Ventas válidas con su ingreso (`revenue`) |
| `output/invalid_sales.csv` | Registros rechazados con sus valores originales y el motivo |
| `output/sales_by_product.csv` | Transacciones, unidades e ingreso total por producto |
| `output/sales_by_city.csv` | Transacciones, unidades e ingreso total por ciudad |
| `output/kpis.json` | Indicadores generales |

Los archivos se sobrescriben en cada ejecución (`mode("overwrite")`). Los ordenamientos tienen un criterio de desempate, así que la misma entrada produce siempre los mismos resultados.

## Resultados

| Indicador | Valor |
|---|---|
| Registros procesados | 15 |
| Registros válidos | 12 |
| Registros inválidos | 3 |
| Ingreso total de ventas válidas | $3.975.000 |
| Unidades vendidas | 24 |
| Producto con mayor ingreso | Monitor ($2.340.000) |
| Ciudad con mayor ingreso | Bogota ($1.900.000) |

**Ingreso por producto:** Monitor $2.340.000 · Audifonos $720.000 · Teclado $510.000 · Mouse $405.000

**Ingreso por ciudad:** Bogota $1.900.000 · Medellin $1.550.000 · Barranquilla $440.000 · Cali $85.000

## Registros inválidos

| order_id | Valor problemático | Motivo |
|---|---|---|
| 1008 | `quantity = 0` | Una venta debe tener al menos una unidad vendida |
| 1009 | `quantity = -1` | Una cantidad negativa no representa una venta |
| 1012 | `unit_price = -780000` | El precio unitario debe ser mayor que cero |

Si se incluyeran, distorsionarían los indicadores. Por ejemplo, el precio negativo de la orden 1012 restaría $780.000 al ingreso total.

## Decisiones de diseño

- **Lectura por encabezado, no por posición**: un esquema explícito asigna los nombres por orden de columna. Leer por encabezado evita etiquetar mal los datos si el archivo trae las columnas en otro orden.
- **Todo como texto al leer**: permite distinguir un campo vacío de un valor mal escrito y registrar el motivo exacto del rechazo.
- **`decimal(18,2)` para el precio**: aritmética exacta para valores monetarios, sin los errores de redondeo de `float`.
- **`cache()`**: el DataFrame validado se reutiliza en varias acciones, y así Spark no vuelve a leer ni validar el CSV cada vez.
=======
# apache-spark-ejercicio
>>>>>>> c3314c4605bf9abb66672d81ad3e51d8a8f97245
