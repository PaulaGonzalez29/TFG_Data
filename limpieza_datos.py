import pandas as pd
import os

# RUTAS
#Direcciones y nombres de datasets
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_ENTRADA = os.path.join(BASE_DIR, "Dataset_Final", "dataset_transversal_2023.csv")
RUTA_SALIDA = os.path.join(BASE_DIR, "Dataset_Final", "dataset_limpio_2023.csv")

#En el análisis de calidad detectamos varias variables y países que debíamos eliminar
# VARIABLES A ELIMINAR
variables_eliminar = [
    "edu_literacy_rate",
    "des_tasa_pobreza",
    "elec_private_investment_energy"
]

# PAÍSES A ELIMINAR
paises_eliminar = [
    "REU", "MTQ", "SHN", "ASM", "XKX", "MAF", "TKL", "NIU",
    "GUM", "IMN", "MNP", "MSR", "COK", "VIR", "AIA",
    "PRK", "CHI", "FRO", "GRL", "PYF"
]

df = pd.read_csv(RUTA_ENTRADA)
print("Dimensión original:", df.shape)

# LIMPIEZA
# Eliminar variables
df = df.drop(columns=variables_eliminar, errors="ignore")
# Eliminar países
df = df[~df["country_code"].isin(paises_eliminar)]
#Revisar dimensión del nuevo dataset
print("Dimensión tras limpieza:", df.shape)

# GUARDAR NUEVO DATASET
df.to_csv(RUTA_SALIDA, index=False, encoding="utf-8-sig")
print("\nDataset limpio guardado en:")
print(RUTA_SALIDA)