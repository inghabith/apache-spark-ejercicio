# Ejercicio práctico: análisis de ventas con Apache Spark

**Nivel:** Junior\
**Duración:** 1 hora y 30 minutos\
**Modalidad:** Individual\
**Tecnología principal:** Apache Spark\
**Contexto de ejecución:** Entorno de Spark disponible para el
ejercicio; si el entorno del entrenamiento incluye MinIO, los archivos
de entrada y salida pueden ubicarse allí.

## 1. Contexto

Una tienda de comercio electrónico necesita revisar las ventas
registradas durante varios días. El equipo recibe un archivo con el
detalle de las transacciones y requiere un resumen confiable para
entender qué productos generan más ingresos, cómo se comportan las
ventas por ciudad y qué registros necesitan revisión.

Actualmente, el equipo realiza este análisis manualmente. El objetivo es
preparar un pequeño proceso de análisis de datos con Apache Spark que
pueda ejecutarse sobre el conjunto de registros suministrado.

## 2. Problema

Construir un proceso de análisis que lea los datos de ventas,
identifique registros válidos y genere indicadores resumidos para apoyar
la toma de decisiones del negocio.

El proceso debe permitir responder, como mínimo, estas preguntas:

-   ¿Cuál fue el ingreso total de las ventas válidas?
-   ¿Cuántas unidades se vendieron?
-   ¿Qué productos generaron más ingresos?
-   ¿Qué ciudad registró el mayor ingreso?
-   ¿Cuántas transacciones válidas y cuántas inválidas se encontraron?

## 3. Datos de entrada

El archivo de entrada representa transacciones de venta. Cada fila
corresponde a un registro.

  Campo           Descripción
  --------------- --------------------------------------------------
  `order_id`      Identificador de la transacción
  `order_date`    Fecha de la transacción, en formato `YYYY-MM-DD`
  `customer_id`   Identificador del cliente
  `city`          Ciudad del cliente
  `product`       Producto vendido
  `quantity`      Unidades registradas
  `unit_price`    Precio por unidad

### Datos de prueba

Los siguientes registros pueden guardarse como un archivo CSV llamado
`sales.csv`.

``` csv
order_id,order_date,customer_id,city,product,quantity,unit_price
1001,2026-09-01,C001,Barranquilla,Teclado,2,85000
1002,2026-09-01,C002,Bogota,Mouse,3,45000
1003,2026-09-02,C003,Medellin,Monitor,1,780000
1004,2026-09-02,C001,Barranquilla,Mouse,2,45000
1005,2026-09-03,C004,Cali,Teclado,1,85000
1006,2026-09-03,C005,Bogota,Monitor,2,780000
1007,2026-09-04,C006,Medellin,Audifonos,2,120000
1008,2026-09-04,C007,Cali,Mouse,0,45000
1009,2026-09-05,C008,Barranquilla,Teclado,-1,85000
1010,2026-09-05,C009,Bogota,Audifonos,1,120000
1011,2026-09-06,C010,Medellin,Teclado,2,85000
1012,2026-09-06,C011,Cali,Monitor,1,-780000
1013,2026-09-07,C012,Barranquilla,Mouse,4,45000
1014,2026-09-07,C013,Bogota,Teclado,1,85000
1015,2026-09-08,C014,Medellin,Audifonos,3,120000
```

Los datos contienen registros que requieren validación. La definición de
qué constituye una venta válida forma parte de los requisitos del
ejercicio.

## 4. Alcance del ejercicio

El resultado debe ser un proceso de análisis de datos que:

-   Procese los registros con Apache Spark.
-   Distinga los registros válidos de los inválidos.
-   Calcule el ingreso por transacción.
-   Genere un resumen de ventas por producto.
-   Genere un resumen de ventas por ciudad.
-   Presente indicadores generales del procesamiento.
-   Deje los resultados disponibles como archivos de salida.

El ejercicio no requiere construir una aplicación web, una API ni un
dashboard.

## 5. Reglas de negocio

1.  Una venta válida debe tener los campos requeridos informados.
2.  La cantidad vendida debe ser mayor que cero.
3.  El precio unitario debe ser mayor que cero.
4.  El ingreso de una transacción corresponde a la cantidad multiplicada
    por el precio unitario.
5.  Los registros inválidos no deben afectar los indicadores de ventas
    válidas.
6.  El resultado debe permitir identificar cuántos registros fueron
    aceptados y cuántos rechazados.

## 6. Criterios de aceptación

El ejercicio se considera terminado cuando se cumplen todos los
siguientes criterios:

-   [ ] El proceso lee el conjunto de datos de entrada sin depender de
    valores escritos manualmente en los cálculos.
-   [ ] Los datos se procesan utilizando Apache Spark.
-   [ ] Los registros válidos e inválidos quedan diferenciados de
    acuerdo con las reglas de negocio.
-   [ ] Se calcula el ingreso por cada venta válida.
-   [ ] Se obtiene el ingreso total de las ventas válidas.
-   [ ] Se obtiene el total de unidades vendidas en las ventas válidas.
-   [ ] Se genera un resumen por producto que incluya unidades vendidas
    e ingreso total.
-   [ ] Se genera un resumen por ciudad que incluya ingreso total.
-   [ ] Se identifica el producto con mayor ingreso.
-   [ ] Se identifica la ciudad con mayor ingreso.
-   [ ] Se informa el número de registros procesados, válidos e
    inválidos.
-   [ ] Los resultados se guardan en archivos de salida que puedan
    consultarse después de ejecutar el proceso.
-   [ ] La ejecución puede repetirse con el mismo archivo de entrada y
    produce resultados consistentes.
-   [ ] El código es legible y los nombres de variables y resultados son
    comprensibles.

## 7. Entregables

Al finalizar el tiempo asignado, se debe entregar:

1.  El código del proceso.
2.  Los archivos de resultados generados.
3.  Un breve resumen con los indicadores solicitados.
4.  Una nota corta que describa qué registros fueron considerados
    inválidos y por qué.

## 8. Distribución sugerida del tiempo

  Actividad                                         Tiempo
  --------------------------------------- ----------------
  Leer el enunciado y revisar los datos         10 minutos
  Desarrollo                                    55 minutos
  Validación de resultados                      15 minutos
  Preparación y entrega                         10 minutos
  **Total**                                 **90 minutos**

## 9. Condiciones

-   El ejercicio es individual.
-   No se requiere experiencia previa con Spark.
-   Se espera que el participante investigue y tome sus propias
    decisiones de implementación.
-   No se exige una interfaz gráfica.
-   Se evaluará el cumplimiento de los criterios de aceptación, la
    consistencia de los resultados y la claridad del código.

**Importante:** el enunciado define el problema y el resultado esperado,
pero no prescribe los pasos técnicos ni la forma de resolverlo.


## 10. Fuentes de datos para trabajar con mayor volumen

Además del CSV pequeño incluido en este enunciado, se pueden utilizar las siguientes fuentes públicas para explorar escenarios con más registros. No es necesario descargar todos los conjuntos de datos para completar el ejercicio dentro de los 90 minutos.

### Opción A — Brazilian E-Commerce de Olist

**Fuente:** [Dataset público de comercio electrónico de Olist en Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Contiene aproximadamente 100.000 pedidos y varios archivos relacionados con clientes, pedidos, productos, pagos, vendedores y reseñas. Permite analizar datos relacionados y explorar cruces entre conjuntos de datos. La descarga puede requerir una cuenta de Kaggle. La licencia indicada es CC BY-NC-SA 4.0; revisa sus condiciones antes de reutilizar o publicar resultados.

Archivos de interés:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`

### Opción B — NYC Taxi & Limousine Commission

**Fuente oficial:** [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Publica registros de viajes de taxi y vehículos de transporte, organizados por mes. Los archivos están disponibles en formato Parquet y pueden contener volúmenes considerables de registros. Es una opción apropiada para experimentar con un archivo mensual y, si el equipo dispone de recursos, ampliar el volumen utilizando varios meses.

Posibles preguntas de análisis:

- ¿Cuántos viajes se registraron por día?
- ¿Cuál fue la distancia promedio de los viajes?
- ¿Cómo varían los importes por tipo de servicio?
- ¿Qué zonas registran más viajes?

Los nombres de columnas pueden variar entre tipos de servicio y periodos; debe consultarse el diccionario de datos publicado por TLC.

### Opción C — API pública para generar datos complementarios

Una API pública puede servir para practicar la ingesta de datos, pero **no es la mejor fuente para simular millones de filas**: las APIs suelen imponer límites de solicitudes, paginación y disponibilidad. Para una prueba de volumen, se recomienda descargar un conjunto de datos estático o generar datos sintéticos de manera controlada.

Si se decide usar una API, documentar la fuente, respetar sus límites y evitar enviar solicitudes masivas que incumplan sus condiciones.

## 11. Experimento de volumen y comparación

Para observar el beneficio de Spark, se propone realizar una prueba adicional con un volumen de datos mayor, utilizando una fuente pública de esta sección o datos sintéticos preparados para el ejercicio.

El experimento debe registrar:

- Volumen procesado: cantidad de filas y tamaño aproximado de los archivos.
- Tiempo total de ejecución de cada prueba.
- Resultado obtenido y validación de que los cálculos sean consistentes.
- Configuración general del entorno donde se ejecutó la prueba.

Cuando el entorno lo permita, comparar el mismo análisis con una implementación sencilla que no utilice Spark y con una implementación en Spark. Si la máquina no permite procesar el volumen elegido con la alternativa tradicional, registrar esa limitación en lugar de presentar una comparación incompleta.

Repetir las pruebas con más de un volumen, por ejemplo:

- Volumen pequeño: hasta 100.000 filas.
- Volumen mediano: alrededor de 500.000 a 1 millón de filas.
- Volumen grande: varios millones de filas, solo si los recursos disponibles lo permiten.

Estos rangos son objetivos orientativos, no una garantía de rendimiento. Los tiempos dependen del equipo, la memoria, el almacenamiento, el formato, la cantidad de columnas y las operaciones realizadas.

### Criterios de aceptación adicionales para el experimento

- [ ] Se documenta la fuente de datos utilizada y su volumen.
- [ ] Se registra el entorno de ejecución.
- [ ] Se mide el tiempo de ejecución de forma consistente.
- [ ] Se comprueba que los resultados de las pruebas sean correctos.
- [ ] Se presenta una comparación de tiempos cuando sea viable.
- [ ] Se explica brevemente qué limitaciones se encontraron y si Spark aportó una ventaja en ese entorno.

**Nota:** Spark no garantiza que cualquier proceso sea más rápido. En volúmenes pequeños, el coste de inicialización puede hacer que una solución sencilla termine antes. El objetivo es observar cómo cambian el tiempo y los recursos al crecer el volumen, no demostrar de antemano que Spark siempre gana.