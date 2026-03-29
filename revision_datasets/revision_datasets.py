import pandas as pd
import os

# =========================================================
# CONFIGURACIÓN DE RUTAS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ruta_educacion = "dataset_educacion_unificado.csv"
ruta_electrificacion = "dataset_electrificacion_unificado.csv"
ruta_desarrollo = "dataset_desarrollo_unificado.csv"

ruta_salida = os.path.join(BASE_DIR, "revision_datasets")
os.makedirs(ruta_salida, exist_ok=True)

ruta_csv_final = os.path.join(ruta_salida, "revision_datasets.csv")

# =========================================================
# FUNCIÓN PARA CARGAR CSV
# =========================================================

def cargar_csv(ruta, nombre):
    print(f"\nCargando {nombre}...")
    print(f"Ruta: {ruta}")

    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontró el archivo de {nombre} en:\n{ruta}")

    df = pd.read_csv(ruta)
    print(f"{nombre} cargado correctamente. Dimensiones: {df.shape}")
    return df

# =========================================================
# FUNCIÓN AUXILIAR PARA AÑADIR FILAS AL RESUMEN
# =========================================================

def agregar_fila(lista_resultados, dataset, tipo_analisis, grupo, variable, valor):
    lista_resultados.append({
        "dataset": dataset,
        "tipo_analisis": tipo_analisis,
        "grupo": grupo,
        "variable": variable,
        "valor": valor
    })

# =========================================================
# FUNCIÓN PRINCIPAL DE ANÁLISIS
# =========================================================

def analizar_dataset(df, nombre_dataset, lista_resultados, umbral_nulos=80):
    print(f"\n{'='*60}")
    print(f"ANALIZANDO {nombre_dataset}")
    print(f"{'='*60}")

    # -------------------------
    # 1. INFORMACIÓN GENERAL
    # -------------------------
    agregar_fila(lista_resultados, nombre_dataset, "info_general", "dataset", "n_filas", df.shape[0])
    agregar_fila(lista_resultados, nombre_dataset, "info_general", "dataset", "n_columnas", df.shape[1])

    # -------------------------
    # 2. TIPOS DE DATOS
    # -------------------------
    for col in df.columns:
        agregar_fila(lista_resultados, nombre_dataset, "tipo_dato", "columna", col, str(df[col].dtype))

    # -------------------------
    # 3. NULOS POR COLUMNA
    # -------------------------
    nulos_col = df.isnull().sum()
    pct_nulos_col = (df.isnull().mean() * 100).round(2)

    for col in df.columns:
        agregar_fila(lista_resultados, nombre_dataset, "nulos_absolutos_columna", "columna", col, int(nulos_col[col]))
        agregar_fila(lista_resultados, nombre_dataset, "nulos_porcentaje_columna", "columna", col, float(pct_nulos_col[col]))

    # -------------------------
    # 4. DUPLICADOS
    # -------------------------
    duplicados_totales = int(df.duplicated().sum())
    agregar_fila(lista_resultados, nombre_dataset, "duplicados", "dataset", "duplicados_totales", duplicados_totales)

    if "country_code" in df.columns and "year" in df.columns:
        duplicados_clave = int(df.duplicated(subset=["country_code", "year"]).sum())
        agregar_fila(lista_resultados, nombre_dataset, "duplicados", "dataset", "duplicados_country_code_year", duplicados_clave)

    # -------------------------
    # 5. VALORES ÚNICOS
    # -------------------------
    unicos = df.nunique()
    for col in df.columns:
        agregar_fila(lista_resultados, nombre_dataset, "valores_unicos", "columna", col, int(unicos[col]))

    # -------------------------
    # 6. NULOS POR AÑO Y COLUMNA
    # -------------------------
    if "year" in df.columns:
        years = sorted(df["year"].dropna().unique())

        for year in years:
            sub = df[df["year"] == year]
            pct_nulos_year = (sub.isnull().mean() * 100).round(2)

            for col in df.columns:
                agregar_fila(
                    lista_resultados,
                    nombre_dataset,
                    "nulos_porcentaje_por_anio",
                    int(year),
                    col,
                    float(pct_nulos_year[col])
                )

            # Resumen global de nulos por año
            pct_total_year = round(sub.isnull().mean().mean() * 100, 2)
            agregar_fila(
                lista_resultados,
                nombre_dataset,
                "resumen_global_nulos_por_anio",
                int(year),
                "porcentaje_nulos_total",
                float(pct_total_year)
            )

    # -------------------------
    # 7. NULOS POR PAÍS Y COLUMNA
    # -------------------------
    if "country_code" in df.columns:
        countries = sorted(df["country_code"].dropna().unique())

        for country in countries:
            sub = df[df["country_code"] == country]
            nulos_country = sub.isnull().sum()

            for col in df.columns:
                agregar_fila(
                    lista_resultados,
                    nombre_dataset,
                    "nulos_absolutos_por_pais",
                    country,
                    col,
                    int(nulos_country[col])
                )

    # -------------------------
    # 8. COLUMNAS CON MÁS DE X% NULOS
    # -------------------------
    columnas_muy_nulas = pct_nulos_col[pct_nulos_col > umbral_nulos].sort_values(ascending=False)

    for col, pct in columnas_muy_nulas.items():
        agregar_fila(
            lista_resultados,
            nombre_dataset,
            f"columnas_mas_de_{umbral_nulos}_pct_nulos",
            "columna",
            col,
            float(pct)
        )

    # -------------------------
    # 9. ESTADÍSTICAS DESCRIPTIVAS NUMÉRICAS
    # -------------------------
    columnas_numericas = df.select_dtypes(include=["number"]).columns

    for col in columnas_numericas:
        serie = df[col].dropna()
        if len(serie) > 0:
            agregar_fila(lista_resultados, nombre_dataset, "estadistica", "columna", f"{col}_mean", round(float(serie.mean()), 4))
            agregar_fila(lista_resultados, nombre_dataset, "estadistica", "columna", f"{col}_std", round(float(serie.std()), 4))
            agregar_fila(lista_resultados, nombre_dataset, "estadistica", "columna", f"{col}_min", round(float(serie.min()), 4))
            agregar_fila(lista_resultados, nombre_dataset, "estadistica", "columna", f"{col}_median", round(float(serie.median()), 4))
            agregar_fila(lista_resultados, nombre_dataset, "estadistica", "columna", f"{col}_max", round(float(serie.max()), 4))

# =========================================================
# EJECUCIÓN
# =========================================================

df_educacion = cargar_csv(ruta_educacion, "educacion")
df_electrificacion = cargar_csv(ruta_electrificacion, "electrificacion")
df_desarrollo = cargar_csv(ruta_desarrollo, "desarrollo")

resultados = []

analizar_dataset(df_educacion, "educacion", resultados, umbral_nulos=80)
analizar_dataset(df_electrificacion, "electrificacion", resultados, umbral_nulos=80)
analizar_dataset(df_desarrollo, "desarrollo", resultados, umbral_nulos=80)

df_resultados = pd.DataFrame(resultados)

# Orden opcional para que quede más limpio
df_resultados = df_resultados.sort_values(
    by=["dataset", "tipo_analisis", "grupo", "variable"]
).reset_index(drop=True)

df_resultados.to_csv(ruta_csv_final, index=False, encoding="utf-8-sig")

print("\n" + "="*60)
print("ANÁLISIS COMPLETO FINALIZADO")
print(f"CSV generado en: {ruta_csv_final}")
print("="*60)