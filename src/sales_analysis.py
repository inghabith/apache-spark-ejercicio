import json
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "data/input/sales.csv"
OUTPUT_DIR = Path("output")

REQUIRED_COLUMNS = ["order_id",
                    "order_date",
                    "customer_id",
                    "city",
                    "product",
                    "quantity",
                    "unit_price"]

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SalesAnalysis")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# =============================================================================
# 1. Lectura
# =============================================================================
df = spark.read.csv(INPUT_PATH, header=True)

missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
if missing_columns:
    spark.stop()
    raise ValueError(f"Missing required columns: {missing_columns}")

df = df.select(REQUIRED_COLUMNS)


# =============================================================================
# 2. Limpieza: quitar espacios y convertir vacíos en null
# =============================================================================
for column in REQUIRED_COLUMNS:
    df = df.withColumn(column, F.trim(F.col(column)))
    df = df.withColumn(
        column,
        F.when(F.col(column) == "", None).otherwise(F.col(column))
    )


# =============================================================================
# 3. Conversión segura de tipos (null si el valor no se puede convertir)
# =============================================================================
df = (
    df.withColumn("quantity_int", F.col("quantity").try_cast("int"))
    .withColumn("unit_price_num", F.col("unit_price").try_cast("decimal(18,2)"))
    .withColumn("order_date_parsed",
                F.to_date(F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd"))))
)


# =============================================================================
# 4. Reglas de negocio: motivo de rechazo y válido / inválido
# =============================================================================
reasons = []

# Regla 1: los campos requeridos deben estar informados
for column in REQUIRED_COLUMNS:
    reasons.append(F.when(F.col(column).isNull(), F.lit(f"campo_vacio:{column}")))

# Formato: el campo trae algo, pero no se pudo convertir a su tipo
reasons.append(F.when(F.col("order_date").isNotNull() & F.col("order_date_parsed").isNull(),
                      F.lit("fecha_invalida")))
reasons.append(F.when(F.col("quantity").isNotNull() & F.col("quantity_int").isNull(),
                      F.lit("quantity_no_es_entero")))
reasons.append(F.when(F.col("unit_price").isNotNull() & F.col("unit_price_num").isNull(),
                      F.lit("unit_price_no_es_numero")))

# Reglas 2 y 3: cantidad y precio mayores que cero
reasons.append(F.when(F.col("quantity_int") <= 0, F.lit("quantity_menor_o_igual_a_cero")))
reasons.append(F.when(F.col("unit_price_num") <= 0, F.lit("unit_price_menor_o_igual_a_cero")))

df = (
    df.withColumn("rejection_reason", F.concat_ws("; ", F.array_compact(F.array(*reasons))))
    .withColumn("is_valid", F.col("rejection_reason") == "")
)

# cache(): este DataFrame se usa varias veces abajo; así Spark no relee
# ni revalida el CSV en cada acción.
df = df.cache()


# =============================================================================
# 5. Ventas válidas (con ingreso) e inválidas (con motivo)
# =============================================================================
# Regla 4: ingreso = cantidad * precio unitario
valid_sales = (
    df.filter(F.col("is_valid"))
    .select(
        "order_id",
        F.col("order_date_parsed").alias("order_date"),
        "customer_id",
        "city",
        "product",
        F.col("quantity_int").alias("quantity"),
        F.col("unit_price_num").alias("unit_price"),
    )
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
)

# Regla 5: los inválidos se separan y NO entran en ningún indicador.
# Se guardan con sus valores originales para poder auditarlos.
invalid_sales = (
    df.filter(~F.col("is_valid"))
    .select(*REQUIRED_COLUMNS, "rejection_reason")
)


# =============================================================================
# 6. Resúmenes por producto y por ciudad
# =============================================================================
# El segundo criterio de orden (nombre) desempata si dos ingresos son iguales,
# así el resultado es siempre el mismo en cada ejecución.
sales_by_product = (
    valid_sales.groupBy("product")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("quantity").alias("units_sold"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"), F.asc("product"))
)

sales_by_city = (
    valid_sales.groupBy("city")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("quantity").alias("units_sold"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"), F.asc("city"))
)


# =============================================================================
# 7. Indicadores generales (KPIs)
# =============================================================================
# Regla 6: cuántos registros se aceptaron y cuántos se rechazaron
records_processed = df.count()
valid_records = valid_sales.count()
invalid_records = invalid_sales.count()

totals = valid_sales.agg(
    F.sum("revenue").alias("total_revenue"),
    F.sum("quantity").alias("total_units"),
).first()

top_product = sales_by_product.first()
top_city = sales_by_city.first()

kpis = {
    "records_processed": records_processed,
    "valid_records": valid_records,
    "invalid_records": invalid_records,
    # float(): Spark devuelve Decimal y json no sabe guardarlo
    "total_revenue": float(totals["total_revenue"] or 0),
    "total_units_sold": int(totals["total_units"] or 0),
    "top_product": top_product["product"] if top_product else None,
    "top_product_revenue": float(top_product["total_revenue"]) if top_product else 0,
    "top_city": top_city["city"] if top_city else None,
    "top_city_revenue": float(top_city["total_revenue"]) if top_city else 0,
}


# =============================================================================
# 8. Escritura de resultados
# =============================================================================
def write_single_csv(dataframe, target_file):
    """Guarda un DataFrame como UN solo archivo CSV con encabezado.

    Spark siempre escribe una CARPETA con archivos part-*.csv (uno por
    partición). Con coalesce(1) queda una sola partición; luego se mueve ese
    part-*.csv a un nombre legible y se borra la carpeta temporal.
    mode("overwrite") reemplaza los resultados si el proceso se vuelve a correr.
    """
    tmp_dir = target_file.parent / f"_tmp_{target_file.stem}"
    dataframe.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp_dir))
    part_file = next(tmp_dir.glob("part-*.csv"))
    target_file.unlink(missing_ok=True)
    shutil.move(str(part_file), str(target_file))
    shutil.rmtree(tmp_dir, ignore_errors=True)


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

write_single_csv(valid_sales.orderBy("order_id"), OUTPUT_DIR / "valid_sales.csv")
write_single_csv(invalid_sales.orderBy("order_id"), OUTPUT_DIR / "invalid_sales.csv")
write_single_csv(sales_by_product, OUTPUT_DIR / "sales_by_product.csv")
write_single_csv(sales_by_city, OUTPUT_DIR / "sales_by_city.csv")

with open(OUTPUT_DIR / "kpis.json", "w", encoding="utf-8") as file:
    json.dump(kpis, file, ensure_ascii=False, indent=2)


# =============================================================================
# 9. Resumen en consola
# =============================================================================
def money(value):
    return f"${value:,.0f}".replace(",", ".")


print("\n" + "=" * 55)
print(" RESUMEN DE VENTAS")
print("=" * 55)
print(f" Registros procesados : {kpis['records_processed']}")
print(f" Registros válidos    : {kpis['valid_records']}")
print(f" Registros inválidos  : {kpis['invalid_records']}")
print(f" Ingreso total        : {money(kpis['total_revenue'])}")
print(f" Unidades vendidas    : {kpis['total_units_sold']}")
print(f" Producto top         : {kpis['top_product']} ({money(kpis['top_product_revenue'])})")
print(f" Ciudad top           : {kpis['top_city']} ({money(kpis['top_city_revenue'])})")
print("=" * 55)

print("\nVentas por producto:")
sales_by_product.show(truncate=False)
print("Ventas por ciudad:")
sales_by_city.show(truncate=False)
print("Registros inválidos:")
invalid_sales.show(truncate=False)

print(f"Resultados guardados en: {OUTPUT_DIR.resolve()}")

df.unpersist()
spark.stop()
