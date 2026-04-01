import pandas as pd
import os
import numpy as np

# =========================================================
# RUTAS
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_2023 = os.path.join(BASE_DIR, "Dataset_Final", "dataset_transversal_2023.csv")
RUTA_UNIFICADO = os.path.join(BASE_DIR, "Dataset_Final", "dataset_unificado.csv")
RUTA_SALIDA = os.path.join(BASE_DIR, "Dataset_Final", "dataset_limpio_2023.csv")

# =========================================================
# VARIABLES Y PAÍSES A ELIMINAR
# =========================================================
variables_eliminar = [
    "edu_literacy_rate",
    "des_tasa_pobreza",
    "elec_private_investment_energy"
]

paises_eliminar = [
    "REU", "MTQ", "SHN", "ASM", "XKX", "MAF", "TKL", "NIU",
    "GUM", "IMN", "MNP", "MSR", "COK", "VIR", "AIA",
    "PRK", "CHI", "FRO", "GRL", "PYF"
]

# =========================================================
# FUNCIÓN 1: ÚLTIMO VALOR DISPONIBLE HASTA 2023
# =========================================================
def obtener_ultimo_valor(df_historico_pais, variable):
    """
    Devuelve el último valor no nulo disponible de una variable
    para un país hasta el año 2023.
    """
    serie = df_historico_pais[["year", variable]].copy()
    serie = serie[serie["year"] <= 2023]
    serie = serie.dropna(subset=[variable])

    if serie.empty:
        return np.nan

    serie = serie.sort_values("year")
    return serie.iloc[-1][variable]

# =========================================================
# FUNCIÓN 2: VALOR DE 2024
# =========================================================
def obtener_valor_2024(df_historico_pais, variable):
    """
    Devuelve el valor de una variable en 2024 para un país,
    si existe y no es nulo.
    """
    serie_2024 = df_historico_pais[
        (df_historico_pais["year"] == 2024) &
        (df_historico_pais[variable].notnull())
    ]

    if serie_2024.empty:
        return np.nan

    return serie_2024.iloc[0][variable]

# =========================================================
# CARGA DE DATOS
# =========================================================
df_2023 = pd.read_csv(RUTA_2023)
df_unificado = pd.read_csv(RUTA_UNIFICADO)

print("Dimensión original dataset 2023:", df_2023.shape)

# =========================================================
# LIMPIEZA INICIAL
# =========================================================
df_2023 = df_2023.drop(columns=variables_eliminar, errors="ignore")
df_unificado = df_unificado.drop(columns=variables_eliminar, errors="ignore")

df_2023 = df_2023[~df_2023["country_code"].isin(paises_eliminar)].copy()
df_unificado = df_unificado[~df_unificado["country_code"].isin(paises_eliminar)].copy()

print("Dimensión tras eliminar variables y países:", df_2023.shape)

# =========================================================
# COLUMNAS NUMÉRICAS
# =========================================================
columnas_numericas = df_2023.select_dtypes(include="number").columns.tolist()

if "year" in columnas_numericas:
    columnas_numericas.remove("year")

# =========================================================
# CONTAR NULOS ANTES
# =========================================================
nulos_antes = df_2023[columnas_numericas].isnull().sum().sum()
print("Nulos antes de rellenar:", nulos_antes)

# =========================================================
# PASO 1: RELLENAR CON EL ÚLTIMO VALOR DISPONIBLE HASTA 2023
# =========================================================
rellenados_hasta_2023 = 0

for idx in df_2023.index:
    pais = df_2023.at[idx, "country_code"]
    df_pais_historico = df_unificado[df_unificado["country_code"] == pais].copy()

    for col in columnas_numericas:
        if pd.isnull(df_2023.at[idx, col]):
            valor = obtener_ultimo_valor(df_pais_historico, col)

            if pd.notnull(valor):
                df_2023.at[idx, col] = valor
                rellenados_hasta_2023 += 1

# =========================================================
# PASO 2: SI SIGUE NULO, INTENTAR CON 2024
# =========================================================
rellenados_2024 = 0

for idx in df_2023.index:
    pais = df_2023.at[idx, "country_code"]
    df_pais_historico = df_unificado[df_unificado["country_code"] == pais].copy()

    for col in columnas_numericas:
        if pd.isnull(df_2023.at[idx, col]):
            valor_2024 = obtener_valor_2024(df_pais_historico, col)

            if pd.notnull(valor_2024):
                df_2023.at[idx, col] = valor_2024
                rellenados_2024 += 1

# =========================================================
# CONTAR NULOS DESPUÉS
# =========================================================
nulos_despues = df_2023[columnas_numericas].isnull().sum().sum()
rellenados_totales = rellenados_hasta_2023 + rellenados_2024

print("Nulos después de rellenar:", nulos_despues)
print("Valores rellenados con histórico hasta 2023:", rellenados_hasta_2023)
print("Valores rellenados con dato de 2024:", rellenados_2024)
print("Valores rellenados totales:", rellenados_totales)

# =========================================================
# GUARDAR DATASET LIMPIO
# =========================================================
df_2023.to_csv(RUTA_SALIDA, index=False, encoding="utf-8-sig")

print("\nDataset limpio guardado en:")
print(RUTA_SALIDA)
print("Dimensión final:", df_2023.shape)