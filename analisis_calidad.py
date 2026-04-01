import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# =========================================================
# CONFIGURACIÓN
# =========================================================
#Direcciones y nombres de datasets
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_ENTRADA = os.path.join(BASE_DIR, "Dataset_Final", "dataset_transversal_2023.csv")
RUTA_SALIDA = os.path.join(BASE_DIR, "Analisis_Calidad")
os.makedirs(RUTA_SALIDA, exist_ok=True)

RUTA_RESUMEN_VARIABLES = os.path.join(RUTA_SALIDA, "resumen_calidad_variables.csv")
RUTA_RESUMEN_PAISES = os.path.join(RUTA_SALIDA, "resumen_nulos_paises.csv")
RUTA_CORRELACIONES = os.path.join(RUTA_SALIDA, "correlaciones_altas.csv")

#Límite establecido para decidir datos problemáticos o de baja calidad
# Umbrales
UMBRAL_NULOS_COLUMNAS = 40
UMBRAL_NULOS_PAISES = 70
UMBRAL_VARIANZA_BAJA = 1e-6
UMBRAL_CORRELACION_ALTA = 0.85

# =========================================================
# FUNCIONES AUXILIARES
# =========================================================
#Cálculo de outliers
def calcular_outliers_iqr(serie: pd.Series) -> tuple[int, float]:
    serie = serie.dropna()
    if len(serie) == 0:
        return 0, 0.0

    q1 = serie.quantile(0.25)
    q3 = serie.quantile(0.75)
    iqr = q3 - q1

    if iqr == 0:
        return 0, 0.0

    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    n_outliers = int(((serie < limite_inferior) | (serie > limite_superior)).sum())
    pct_outliers = round((n_outliers / len(serie)) * 100, 2)

    return n_outliers, pct_outliers

#Gráficos
def guardar_grafico_barras(serie, titulo, xlabel, ylabel, ruta_archivo, top_n=None):
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
    plt.savefig(ruta_archivo, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Gráfico guardado: {ruta_archivo}")


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

if "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

#Convertir a texto, eliminar espacios y poner en mayúsculas. Evitar varios códigos
#de países iguales pero en distintos formatos
df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
#Eliminar filas con código NaN o vacíos
df = df[df["country_code"].notna()]
df = df[df["country_code"] != ""]
df = df[df["country_code"].str.lower() != "nan"]

#Revisar duplicados de países y eliminar
duplicados_country = int(df.duplicated(subset=["country_code"]).sum())
if duplicados_country > 0:
    print(f"Se detectaron {duplicados_country} países duplicados. Se conserva la primera fila.")
    df = df.drop_duplicates(subset=["country_code"], keep="first").copy()

print("Dimensión tras primera limpieza de posibles errores:", df.shape)

# =========================================================
# ANÁLISIS
# =========================================================
columnas_excluir = ["country_code", "year"]
columnas_analisis = [c for c in df.columns if c not in columnas_excluir]

columnas_numericas = df[columnas_analisis].select_dtypes(include=[np.number]).columns.tolist()

print(f"Número de variables analizadas: {len(columnas_analisis)}")
print(f"Número de variables numéricas: {len(columnas_numericas)}")

# 1. NULOS POR VARIABLE
nulos_col = (df[columnas_analisis].isnull().mean() * 100).round(2)

# 2. NULOS POR PAÍS
nulos_pais = (df[columnas_analisis].isnull().mean(axis=1) * 100).round(2)
df_paises = pd.DataFrame({
    "country_code": df["country_code"],
    "pct_nulos": nulos_pais,
    "pais_problematico": nulos_pais >= UMBRAL_NULOS_PAISES
}).sort_values(by="pct_nulos", ascending=False)

# 3. VARIANZA Y DESVIACIÓN TÍPICA
varianzas = df[columnas_numericas].var(numeric_only=True)
stds = df[columnas_numericas].std(numeric_only=True)

# 4. OUTLIERS POR VARIABLE
outliers_info = {}
for col in columnas_numericas:
    n_outliers, pct_outliers = calcular_outliers_iqr(df[col])
    outliers_info[col] = {
        "n_outliers": n_outliers,
        "pct_outliers": pct_outliers
    }

# 5. CORRELACIONES
#Nos centramos en correlaciones altas
correlaciones_altas = []
variables_con_corr_alta = set()

if len(columnas_numericas) >= 2:
    corr = df[columnas_numericas].corr().abs()

    for i in range(len(corr.columns)):
        for j in range(i + 1, len(corr.columns)):
            var_1 = corr.columns[i]
            var_2 = corr.columns[j]
            valor_corr = corr.iloc[i, j]

            if pd.notna(valor_corr) and valor_corr >= UMBRAL_CORRELACION_ALTA:
                correlaciones_altas.append({
                    "variable_1": var_1,
                    "variable_2": var_2,
                    "correlacion_abs": round(float(valor_corr), 4)
                })
                variables_con_corr_alta.add(var_1)
                variables_con_corr_alta.add(var_2)

df_corr_altas = pd.DataFrame(correlaciones_altas).sort_values(
    by="correlacion_abs", ascending=False
) if correlaciones_altas else pd.DataFrame(columns=["variable_1", "variable_2", "correlacion_abs"])

# 6. RESUMEN FINAL POR VARIABLE
resumen_variables = []

for col in columnas_analisis:
    es_numerica = col in columnas_numericas
    pct_nulos = float(nulos_col[col])

    varianza = float(varianzas[col]) if es_numerica and col in varianzas.index and pd.notna(varianzas[col]) else np.nan
    std = float(stds[col]) if es_numerica and col in stds.index and pd.notna(stds[col]) else np.nan

    n_outliers = outliers_info[col]["n_outliers"] if es_numerica else np.nan
    pct_outliers = outliers_info[col]["pct_outliers"] if es_numerica else np.nan

    correlacion_alta = col in variables_con_corr_alta if es_numerica else False
    baja_varianza = pd.notna(varianza) and varianza <= UMBRAL_VARIANZA_BAJA

    motivo = []
    if pct_nulos > UMBRAL_NULOS_COLUMNAS:
        motivo.append(f"nulos>{UMBRAL_NULOS_COLUMNAS}%")
    if baja_varianza:
        motivo.append("baja_varianza")
    if correlacion_alta:
        motivo.append("correlacion_alta")

    resumen_variables.append({
        "variable": col,
        "tipo": "numerica" if es_numerica else "no_numerica",
        "pct_nulos": round(pct_nulos, 2),
        "varianza": round(varianza, 6) if pd.notna(varianza) else np.nan,
        "std": round(std, 6) if pd.notna(std) else np.nan,
        "n_outliers": n_outliers,
        "pct_outliers": pct_outliers,
        "correlacion_alta": correlacion_alta,
        "motivo": "; ".join(motivo) if motivo else "sin_problemas_graves"
    })

df_resumen_variables = pd.DataFrame(resumen_variables).sort_values(
    by=["pct_nulos", "variable"],
    ascending=[False, True]
)

# GUARDAR CSV
df_resumen_variables.to_csv(RUTA_RESUMEN_VARIABLES, index=False, encoding="utf-8-sig")
df_paises.to_csv(RUTA_RESUMEN_PAISES, index=False, encoding="utf-8-sig")
df_corr_altas.to_csv(RUTA_CORRELACIONES, index=False, encoding="utf-8-sig")

print("\n" + "=" * 70)
print("ANÁLISIS DE CALIDAD FINALIZADO")
print(f"Resumen de variables: {RUTA_RESUMEN_VARIABLES}")
print(f"Resumen de países:    {RUTA_RESUMEN_PAISES}")
print(f"Correlaciones altas:  {RUTA_CORRELACIONES}")
print("=" * 70)

# GRÁFICOS NECESARIOS
# 1. Top variables con más nulos
serie_columnas = nulos_col.sort_values(ascending=False)
guardar_grafico_barras(
    serie=serie_columnas,
    titulo="Top variables con mayor porcentaje de nulos",
    xlabel="Variable",
    ylabel="% de nulos",
    ruta_archivo=os.path.join(RUTA_SALIDA, "top_variables_mas_nulas.png"),
    top_n=20
)
# 2. Top países con más nulos
serie_paises = pd.Series(nulos_pais.values, index=df["country_code"]).sort_values(ascending=False)
guardar_grafico_barras(
    serie=serie_paises,
    titulo="Top países con mayor porcentaje de nulos",
    xlabel="País",
    ylabel="% de nulos",
    ruta_archivo=os.path.join(RUTA_SALIDA, "top_paises_mas_nulos.png"),
    top_n=20
)
# 3. Mapa de calor de nulos
plt.figure(figsize=(18, 10))
matriz_nulos = df[columnas_analisis].isnull().astype(int)

plt.imshow(matriz_nulos, aspect="auto")
plt.colorbar(label="Nulos (1 = sí, 0 = no)")
plt.title("Heatmap de valores nulos")
plt.xlabel("Variables")
plt.ylabel("Países")
plt.xticks(range(len(columnas_analisis)), columnas_analisis, rotation=90)
plt.yticks([])
plt.tight_layout()

ruta_heatmap = os.path.join(RUTA_SALIDA, "heatmap_nulos.png")
plt.savefig(ruta_heatmap, dpi=300, bbox_inches="tight")
plt.close()
print(f"Gráfico guardado: {ruta_heatmap}")

# 4. Matriz de correlación solo para variables numéricas
if len(columnas_numericas) >= 2:
    corr = df[columnas_numericas].corr()

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

# 5. Histogramas y boxplots solo de variables numéricas
ruta_hist = os.path.join(RUTA_SALIDA, "Histogramas")
ruta_box = os.path.join(RUTA_SALIDA, "Boxplots")
os.makedirs(ruta_hist, exist_ok=True)
os.makedirs(ruta_box, exist_ok=True)

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
    ruta_archivo_hist = os.path.join(ruta_hist, f"hist_{col}.png")
    plt.savefig(ruta_archivo_hist, dpi=300, bbox_inches="tight")
    plt.close()

    plt.figure(figsize=(8, 6))
    plt.boxplot(serie, vert=True)
    plt.title(f"Boxplot - {col}")
    plt.ylabel(col)
    plt.tight_layout()
    ruta_archivo_box = os.path.join(ruta_box, f"boxplot_{col}.png")
    plt.savefig(ruta_archivo_box, dpi=300, bbox_inches="tight")
    plt.close()

print("\nProceso completado correctamente.")