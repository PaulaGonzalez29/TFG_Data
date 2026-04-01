import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

# =========================================================
# RUTAS
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_DATASET = os.path.join(BASE_DIR, "Dataset_Final", "dataset_limpio_2023.csv")

# =========================================================
# CARGA DE DATOS
# =========================================================
df = pd.read_csv(RUTA_DATASET)

print("Dimensión del dataset:", df.shape)

# =========================================================
# 1. VISIÓN GENERAL
# =========================================================
print("\nInformación general:")
print(df.info())

print("\nPrimeras filas:")
print(df.head())

print("\nEstadísticas básicas:")
print(df.describe())

# =========================================================
# 2. VALIDACIÓN BÁSICA
# =========================================================
print("\n================ VALIDACIÓN BÁSICA ================\n")

duplicados = df.duplicated().sum()
print("Duplicados totales:", duplicados)

columnas_electricidad = [
    "elec_electricity_access_total",
    "elec_electricity_access_rural",
    "elec_electricity_access_urban"
]

for col in columnas_electricidad:
    if col in df.columns:
        fuera_rango = df[(df[col] < 0) | (df[col] > 100)]
        print(f"Valores fuera de rango en {col}: {len(fuera_rango)}")

# =========================================================
# 3. VARIABLES RELEVANTES PARA LOS GRÁFICOS
# =========================================================
variables_graficos = [
    "elec_electricity_access_total",
    "elec_electricity_access_rural",
    "des_gdp_per_capita",
    "des_hdi"
]

variables_graficos = [v for v in variables_graficos if v in df.columns]

nombres_legibles = {
    "elec_electricity_access_total": "Acceso a electricidad total (%)",
    "elec_electricity_access_rural": "Acceso a electricidad rural (%)",
    "elec_electricity_access_urban": "Acceso a electricidad urbano (%)",
    "des_gdp_per_capita": "PIB per cápita",
    "des_hdi": "Índice de Desarrollo Humano (HDI)"
}

# =========================================================
# 4. HISTOGRAMAS (4 EN UNA MISMA HOJA)
# =========================================================
if len(variables_graficos) > 0:
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    for i, col in enumerate(variables_graficos):
        axes[i].hist(df[col].dropna(), bins=20)
        axes[i].set_title(f"Distribución de {nombres_legibles.get(col, col)}")
        axes[i].set_xlabel(nombres_legibles.get(col, col))
        axes[i].set_ylabel("Número de países")

    for j in range(len(variables_graficos), 4):
        axes[j].set_visible(False)

    fig.suptitle("Histogramas de variables relevantes", fontsize=14)
    plt.tight_layout()
    plt.show()

# =========================================================
# 5. BOXPLOTS (4 EN UNA MISMA HOJA)
# =========================================================
if len(variables_graficos) > 0:
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    for i, col in enumerate(variables_graficos):
        sns.boxplot(x=df[col], ax=axes[i])
        axes[i].set_title(f"Boxplot de {nombres_legibles.get(col, col)}")
        axes[i].set_xlabel(nombres_legibles.get(col, col))

    for j in range(len(variables_graficos), 4):
        axes[j].set_visible(False)

    fig.suptitle("Boxplots de variables relevantes", fontsize=14)
    plt.tight_layout()
    plt.show()

# =========================================================
# 6. MATRIZ DE CORRELACIÓN DE TODAS LAS VARIABLES NUMÉRICAS
# =========================================================
columnas_numericas = df.select_dtypes(include="number").columns.tolist()

if "year" in columnas_numericas:
    columnas_numericas.remove("year")

if len(columnas_numericas) > 1:
    corr = df[columnas_numericas].corr()

    plt.figure(figsize=(16, 12))
    sns.heatmap(corr, cmap="coolwarm", center=0)
    plt.title("Matriz de correlación de todas las variables numéricas")
    plt.tight_layout()
    plt.show()

# =========================================================
# 7. SCATTER PLOTS (HASTA 4 EN UNA MISMA HOJA)
# =========================================================
variables_scatter = [
    "elec_electricity_access_rural",
    "des_gdp_per_capita",
    "des_hdi"
]

variables_scatter = [v for v in variables_scatter if v in df.columns]

if "elec_electricity_access_total" in df.columns and len(variables_scatter) > 0:
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    for i, col in enumerate(variables_scatter):
        sns.scatterplot(
            data=df,
            x="elec_electricity_access_total",
            y=col,
            ax=axes[i]
        )
        axes[i].set_title(f"Electricidad total vs {nombres_legibles.get(col, col)}")
        axes[i].set_xlabel("Acceso a electricidad total (%)")
        axes[i].set_ylabel(nombres_legibles.get(col, col))

    for j in range(len(variables_scatter), 4):
        axes[j].set_visible(False)

    fig.suptitle("Relación entre acceso a electricidad y variables relevantes", fontsize=14)
    plt.tight_layout()
    plt.show()

# =========================================================
# 8. COMPARACIÓN POR GRUPOS DE HDI
# =========================================================
if "des_hdi" in df.columns and "elec_electricity_access_total" in df.columns:
    df["grupo_hdi"] = pd.qcut(df["des_hdi"], 3, labels=["Bajo", "Medio", "Alto"])

    plt.figure(figsize=(8, 5))
    sns.boxplot(data=df, x="grupo_hdi", y="elec_electricity_access_total")
    plt.title("Acceso a electricidad según nivel de desarrollo humano")
    plt.xlabel("Grupo de HDI")
    plt.ylabel("Acceso a electricidad total (%)")
    plt.tight_layout()
    plt.show()