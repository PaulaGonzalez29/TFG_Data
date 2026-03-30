import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# =========================================================
# CONFIGURACIÓN
# =========================================================
#Direcciones y nombres de datasets
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_ENTRADA = os.path.join(BASE_DIR, "Dataset_Final", "dataset_transversal_2023.csv")
RUTA_SALIDA = os.path.join(BASE_DIR, "Analisis_Calidad")
os.makedirs(RUTA_SALIDA, exist_ok=True)

RUTA_CSV_FINAL = os.path.join(RUTA_SALIDA, "analisis_calidad_completo.csv")

#Límite establecido para decidir datos problemáticos o de baja calidad
#Variable global
UMBRAL_NULOS_PAISES = 70
UMBRAL_NULOS_COLUMNAS = 70

# =========================================================
# FUNCIONES AUXILIARES
# =========================================================
#Añadir filas a nuestro csv de análisis de calidad
def agregar_fila(lista_resultados, tipo_analisis, grupo, variable, valor, detalle=""):
    lista_resultados.append({
        "tipo_analisis": tipo_analisis,
        "grupo": grupo,
        "variable": variable,
        "valor": valor,
        "detalle": detalle
    })

#Cálculo de outliers
def calcular_outliers_iqr(df, columnas_numericas):
    resultados = []

    for col in columnas_numericas:
        serie = df[col].dropna()

        if len(serie) == 0:
            continue

        q1 = serie.quantile(0.25)
        q3 = serie.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0:
            n_outliers = 0
            pct_outliers = 0.0
            lower = q1
            upper = q3
        else:
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask_outliers = (serie < lower) | (serie > upper)
            n_outliers = int(mask_outliers.sum())
            pct_outliers = round((n_outliers / len(serie)) * 100, 2)

        resultados.append({
            "variable": col,
            "q1": round(float(q1), 4),
            "q3": round(float(q3), 4),
            "iqr": round(float(iqr), 4),
            "limite_inferior": round(float(lower), 4),
            "limite_superior": round(float(upper), 4),
            "n_outliers": n_outliers,
            "pct_outliers": pct_outliers
        })

    return resultados

#Gráficos
def guardar_grafico_barras(serie, titulo, xlabel, ylabel, nombre_archivo, top_n=None):
    datos = serie.copy()

    if top_n is not None:
        datos = datos.head(top_n)

    plt.figure(figsize=(12, 7))
    datos.plot(kind="bar")
    plt.title(titulo)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=90)
    plt.tight_layout()

    ruta = os.path.join(RUTA_SALIDA, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico guardado: {ruta}")

# Cargar dataset y leer
if not os.path.exists(RUTA_ENTRADA):
    raise FileNotFoundError(f"No se encontró el archivo:\n{RUTA_ENTRADA}")

df = pd.read_csv(RUTA_ENTRADA)

print("\nDataset cargado correctamente.")
print("Dimensión inicial:", df.shape)

# LIMPIEZA BÁSICA

df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

#Revisar que hay tanto código de país como año
if "country_code" not in df.columns:
    raise ValueError("Falta la columna 'country_code' en el dataset.")

if "year" not in df.columns:
    raise ValueError("Falta la columna 'year' en el dataset.")

#Convertir a texto, eliminar espacios y poner en mayúsculas. Evitar varios códigos
#de países iguales pero en distintos formatos
df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
#Eliminar filas con código NaN o vacíos
df = df[df["country_code"].notna()]
df = df[df["country_code"] != ""]
df = df[df["country_code"].str.lower() != "nan"]

#Convertir valores de años a números, y eliminar 
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df = df.dropna(subset=["year"])
df["year"] = df["year"].astype(int)

#Revisar duplicados de países y eliminar
duplicados_country = int(df.duplicated(subset=["country_code"]).sum())
df = df.drop_duplicates(subset=["country_code"], keep="first").copy()

print("Dimensión tras limpieza básica:", df.shape)

# =========================================================
# ANÁLISIS
# =========================================================

resultados = []

# ------------------------------------
# 1. INFORMACIÓN GENERAL DEL DATASET
# ------------------------------------
agregar_fila(resultados, "info_general", "dataset", "n_filas", df.shape[0])
agregar_fila(resultados, "info_general", "dataset", "n_columnas", df.shape[1])
agregar_fila(resultados, "info_general", "dataset", "n_paises_unicos", df["country_code"].nunique())
agregar_fila(resultados, "info_general", "dataset", "duplicados_country_code_detectados", duplicados_country)

# -----------------------------------
# 2. PORCENTAJE DE NULOS POR COLUMNA
# -----------------------------------
#Cálculo de nulos
nulos_col = (df.isnull().mean() * 100).round(2)
#Agregar resultado
for col in df.columns:
    agregar_fila(resultados, "nulos_porcentaje_columna", "columna", col, float(nulos_col[col]))

#Revisar si consideramos que hay columnas problemáticas, >70%
for col in df.columns:
    if nulos_col[col] >= UMBRAL_NULOS_COLUMNAS:
        agregar_fila(
            resultados,
            "columnas_problematicas",
            "columna",
            col,
            float(nulos_col[col])
        )

# -------------------------
# 3. NULOS POR PAÍS
# -------------------------
#Mismos pasos que en el código anterior, pero analizando posibles países problemáticos
columnas_analisis = [col for col in df.columns if col not in ["country_code", "year"]]

nulos_pais = df[columnas_analisis].isnull().mean(axis=1).mul(100).round(2)

for i, row in df.iterrows():
    pais = row["country_code"]

    agregar_fila(resultados, "nulos_pais", pais, "porcentaje_nulos", float(nulos_pais.loc[i]))
    agregar_fila(resultados, "n_variables_analizadas_pais", pais, "n_variables_analizadas", len(columnas_analisis))

    if nulos_pais.loc[i] >= UMBRAL_NULOS_PAISES:
        agregar_fila(
            resultados,
            "paises_problematicos",
            pais,
            "porcentaje_nulos",
            float(nulos_pais.loc[i])
        )

# -------------------------
# 4. DUPLICADOS
# -------------------------
#Revisión de filas duplicadas y posibles países duplicadas
duplicados_totales = int(df.duplicated().sum())
agregar_fila(resultados, "duplicados", "dataset", "duplicados_totales", duplicados_totales)
agregar_fila(resultados, "duplicados", "dataset", "duplicados_country_code", int(df.duplicated(subset=["country_code"]).sum()))

# -------------------------
# 5. VALORES ÚNICOS
# -------------------------
columnas_numericas = df.select_dtypes(include=[np.number]).columns.tolist()
columnas_numericas = [c for c in columnas_numericas if c != "year"]

agregar_fila(resultados, "resumen_variables_numericas", "dataset", "n_variables_numericas", len(columnas_numericas))

# -----------------------------------------------------------------------------
# 6. ESTADÍSTICAS PRINCIPALES (MEDIA, MEDIANA, MIN, MAX Y DESVIACIÓN ESTÁNDAR)
# -----------------------------------------------------------------------------
#Los representaremos en gráficas para analizar
n_columnas_problematicas = int((nulos_col >= UMBRAL_NULOS_COLUMNAS).sum())
n_paises_problematicos = int((nulos_pais >= UMBRAL_NULOS_PAISES).sum())

agregar_fila(resultados, "resumen_final", "dataset", "n_paises_finales", df["country_code"].nunique())
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_totales", len(df.columns))
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_analizadas", len(columnas_analisis))
agregar_fila(resultados, "resumen_final", "dataset", "n_columnas_problematicas", n_columnas_problematicas)
agregar_fila(resultados, "resumen_final", "dataset", "n_paises_problematicos", n_paises_problematicos)
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_numericas", len(columnas_numericas))
# -------------------------
# 8. OUTLIERS
# -------------------------
#Representación de outliers en boxplots
outliers_resultados = calcular_outliers_iqr(df, columnas_numericas)

for item in outliers_resultados:
    variable = item["variable"]
    agregar_fila(resultados, "outliers", "columna", f"{variable}_q1", item["q1"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_q3", item["q3"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_iqr", item["iqr"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_limite_inferior", item["limite_inferior"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_limite_superior", item["limite_superior"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_n_outliers", item["n_outliers"])
    agregar_fila(resultados, "outliers", "columna", f"{variable}_pct_outliers", item["pct_outliers"])

# -------------------------
# 9. RESUMEN FINAL
# -------------------------
n_columnas_problematicas = int((nulos_col >= UMBRAL_NULOS_COLUMNAS).sum())
n_paises_problematicos = int((nulos_pais >= UMBRAL_NULOS_PAISES).sum())
n_variables_con_outliers = sum(1 for item in outliers_resultados if item["n_outliers"] > 0)
#Resumen de resultados calculados
agregar_fila(resultados, "resumen_final", "dataset", "n_paises_finales", df["country_code"].nunique())
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_totales", len(df.columns))
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_analizadas", len(columnas_analisis))
agregar_fila(resultados, "resumen_final", "dataset", "n_columnas_problematicas", n_columnas_problematicas)
agregar_fila(resultados, "resumen_final", "dataset", "n_paises_problematicos", n_paises_problematicos)
agregar_fila(resultados, "resumen_final", "dataset", "n_variables_numericas", len(columnas_numericas))

# =========================================================
# GUARDAR EN CSV
# =========================================================
df_resultados = pd.DataFrame(resultados)

df_resultados = df_resultados.sort_values(
    by=["tipo_analisis", "grupo", "variable"]
).reset_index(drop=True)

df_resultados.to_csv(RUTA_CSV_FINAL, index=False, encoding="utf-8-sig")

print("\n" + "=" * 70)
print("ANÁLISIS DE CALIDAD FINALIZADO")
print(f"CSV único generado en: {RUTA_CSV_FINAL}")
print("=" * 70)

# =========================================================
# GRÁFICOS
# =========================================================

# Top países con más nulos
serie_paises = pd.Series(nulos_pais.values, index=df["country_code"]).sort_values(ascending=False)
guardar_grafico_barras(
    serie_paises,
    "Top países con mayor porcentaje de nulos",
    "País",
    "% de nulos",
    "top_paises_mas_nulos.png",
    top_n=20
)

# Top columnas con más nulos
serie_columnas = nulos_col.sort_values(ascending=False)
guardar_grafico_barras(
    serie_columnas,
    "Top variables con mayor porcentaje de nulos",
    "Variable",
    "% de nulos",
    "top_columnas_mas_nulas.png",
    top_n=20
)

# Histogramas de variables numéricas
ruta_hist = os.path.join(RUTA_SALIDA, "Histogramas")
os.makedirs(ruta_hist, exist_ok=True)

for col in columnas_numericas:
    serie = df[col].dropna()

    if len(serie) == 0:
        continue

    plt.figure(figsize=(10, 6))
    plt.hist(serie, bins=20)
    plt.title(f"Histograma - {col}")
    plt.xlabel(col)
    plt.ylabel("Frecuencia")
    plt.tight_layout()

    ruta_archivo = os.path.join(ruta_hist, f"hist_{col}.png")
    plt.savefig(ruta_archivo, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico guardado: {ruta_archivo}")

# Boxplots de variables numéricas
ruta_box = os.path.join(RUTA_SALIDA, "Boxplots")
os.makedirs(ruta_box, exist_ok=True)

for col in columnas_numericas:
    serie = df[col].dropna()

    if len(serie) == 0:
        continue

    plt.figure(figsize=(8, 6))
    plt.boxplot(serie, vert=True)
    plt.title(f"Boxplot - {col}")
    plt.ylabel(col)
    plt.tight_layout()

    ruta_archivo = os.path.join(ruta_box, f"boxplot_{col}.png")
    plt.savefig(ruta_archivo, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico guardado: {ruta_archivo}")

# Matriz de correlación solo en gráfico
if len(columnas_numericas) >= 2:
    corr = df[columnas_numericas].corr(numeric_only=True)

    plt.figure(figsize=(16, 12))
    plt.imshow(corr, aspect="auto")
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Matriz de correlación")
    plt.tight_layout()

    ruta_corr = os.path.join(RUTA_SALIDA, "matriz_correlacion.png")
    plt.savefig(ruta_corr, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico guardado: {ruta_corr}")

# Heatmap de valores nulos
plt.figure(figsize=(18, 10))

# Convertimos a 0 y 1 (1 = nulo)
matriz_nulos = df.isnull().astype(int)

plt.imshow(matriz_nulos, aspect="auto")
plt.colorbar(label="Nulos (1 = sí, 0 = no)")

plt.title("Heatmap de valores nulos")
plt.xlabel("Variables")
plt.ylabel("Países")

plt.xticks(range(len(df.columns)), df.columns, rotation=90)
plt.yticks([])  # quitamos países para que no se sature

plt.tight_layout()

ruta_heatmap = os.path.join(RUTA_SALIDA, "heatmap_nulos.png")
plt.savefig(ruta_heatmap, dpi=300, bbox_inches="tight")
plt.close()

print(f"Gráfico guardado: {ruta_heatmap}")