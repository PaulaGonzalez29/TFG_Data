import pandas as pd
import os

# =========================================================
# RUTAS
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTAS_POSIBLES = [
    BASE_DIR,
    os.path.join(BASE_DIR, "Datos TFG")
]

# =========================================================
# ARCHIVOS DE ENTRADA
# =========================================================
archivos_datasets = {
    "dataset_educacion_unificado.csv": "educacion",
    "dataset_desarrollo_unificado.csv": "desarrollo",
    "dataset_electrificacion_unificado.csv": "electrificacion"
}

# =========================================================
# REGIONES / AGREGADOS A EXCLUIR
# =========================================================
REGIONES_EXCLUIR = {
    "AFE", "AFW", "ARB", "CEB", "CSS", "EAP", "EAR", "EAS", "ECA", "ECS",
    "EMU", "EUU", "FCS", "HIC", "HPC", "IBD", "IBT", "IDA", "IDB", "IDX",
    "INX", "LAC", "LCN", "LDC", "LIC", "LMC", "LMY", "LTE", "MEA", "MIC",
    "MNA", "NAC", "OED", "OSS", "PRE", "PSS", "PST", "SAS", "SSA", "SSF",
    "SST", "TEA", "TEC", "TLA", "TMN", "TSA", "TSS", "UMC", "WLD"
}

# =========================================================
# FUNCIÓN PARA BUSCAR ARCHIVOS
# =========================================================
def buscar_archivo(nombre_archivo):
    for ruta_base in RUTAS_POSIBLES:
        ruta_completa = os.path.join(ruta_base, nombre_archivo)
        if os.path.exists(ruta_completa):
            return ruta_completa
    return None

# =========================================================
# FUNCIÓN PARA LEER Y LIMPIAR CADA DATASET HISTÓRICO
# =========================================================
def leer_dataset_historico(ruta_archivo, nombre_dataset):
    df = pd.read_csv(ruta_archivo)

    # Normalizar nombres de columnas
    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    # Renombrar columnas si hace falta
    renombrados = {
        "Country Code": "country_code",
        "Country code": "country_code",
        "country code": "country_code",
        "iso_code": "country_code",
        "ISO Code": "country_code",
        "Year": "year",
        "Año": "year",
        "año": "year",
        "Anio": "year",
        "anio": "year"
    }
    df = df.rename(columns=renombrados)

    # Comprobar columnas necesarias
    if "country_code" not in df.columns:
        raise ValueError(
            f"En {ruta_archivo} falta la columna 'country_code'. "
            f"Columnas detectadas: {list(df.columns)}"
        )

    if "year" not in df.columns:
        raise ValueError(
            f"En {ruta_archivo} falta la columna 'year'. "
            f"Columnas detectadas: {list(df.columns)}"
        )

    # Limpiar country_code
    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
    df = df[df["country_code"].notna()]
    df = df[df["country_code"] != ""]
    df = df[df["country_code"].str.lower() != "nan"]

    # Eliminar regiones/agregados
    filas_antes = len(df)
    df = df[~df["country_code"].isin(REGIONES_EXCLUIR)].copy()
    filas_despues = len(df)
    filas_eliminadas = filas_antes - filas_despues

    if filas_eliminadas > 0:
        print(f"\nEn {nombre_dataset} se eliminaron {filas_eliminadas} filas de regiones/agregados.")

    # Mantener solo códigos ISO de 3 letras
    df = df[df["country_code"].str.len() == 3].copy()

    # Convertir year a numérico
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Eliminar columnas de texto que no aportan
    columnas_eliminar = ["Country Name", "country_name", "dataset", "fuente"]
    df = df.drop(columns=columnas_eliminar, errors="ignore")

    # Eliminar columnas completamente vacías
    columnas_validas = ["country_code", "year"]
    columnas_excluidas = []

    for col in df.columns:
        if col in ["country_code", "year"]:
            continue

        if df[col].isnull().all():
            columnas_excluidas.append(col)
        else:
            columnas_validas.append(col)

    df = df[columnas_validas]

    if columnas_excluidas:
        print(f"\nColumnas no incluidas en {nombre_dataset} por estar completamente vacías:")
        for col in columnas_excluidas:
            print(f"- {col}")

    # Eliminar duplicados exactos de país-año
    df = df.drop_duplicates(subset=["country_code", "year"], keep="first")

    # Añadir prefijo a variables
    columnas_renombrar = {}
    for col in df.columns:
        if col not in ["country_code", "year"]:
            if nombre_dataset == "educacion":
                columnas_renombrar[col] = f"edu_{col}"
            elif nombre_dataset == "desarrollo":
                columnas_renombrar[col] = f"des_{col}"
            elif nombre_dataset == "electrificacion":
                columnas_renombrar[col] = f"elec_{col}"

    df = df.rename(columns=columnas_renombrar)

    return df

# =========================================================
# CARGAR DATASETS
# =========================================================
lista_dfs = []

for archivo, nombre_dataset in archivos_datasets.items():
    ruta_archivo = buscar_archivo(archivo)

    if ruta_archivo is None:
        print(f"No encontrado: {archivo}")
        continue

    try:
        df = leer_dataset_historico(ruta_archivo, nombre_dataset)
        lista_dfs.append(df)
        print(f"OK: {archivo} -> {ruta_archivo}")
        print(f"Dimensión de {nombre_dataset}: {df.shape}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")

# =========================================================
# UNIFICAR DATASETS POR PAÍS Y AÑO
# =========================================================
df_final = lista_dfs[0]

for df in lista_dfs[1:]:
    df_final = df_final.merge(df, on=["country_code", "year"], how="outer")

# =========================================================
# LIMPIEZA FINAL
# =========================================================
df_final = df_final.dropna(subset=["country_code", "year"])

df_final["country_code"] = df_final["country_code"].astype(str).str.strip().str.upper()
df_final = df_final[df_final["country_code"] != ""]
df_final = df_final[df_final["country_code"].str.lower() != "nan"]

df_final["year"] = pd.to_numeric(df_final["year"], errors="coerce")
df_final = df_final.dropna(subset=["year"])
df_final["year"] = df_final["year"].astype(int)

# Seguridad extra
df_final = df_final[~df_final["country_code"].isin(REGIONES_EXCLUIR)].copy()
df_final = df_final[df_final["country_code"].str.len() == 3].copy()

# Eliminar duplicados finales por país-año
df_final = df_final.drop_duplicates(subset=["country_code", "year"], keep="first")

# Ordenar
df_final = df_final.sort_values(["country_code", "year"]).reset_index(drop=True)

# =========================================================
# REVISIÓN FINAL
# =========================================================
print("\nPrimeras filas:")
print(df_final.head())

print("\nInformación general:")
print(df_final.info())

print("\nDimensión final:")
print(df_final.shape)

print("\nAños disponibles:")
print(sorted(df_final["year"].unique()))

print("\nNúmero de países únicos:")
print(df_final["country_code"].nunique())

print("\nPorcentaje de nulos por columna:")
print((df_final.isnull().mean() * 100).round(2).sort_values(ascending=False))

# =========================================================
# GUARDAR DATASET FINAL
# =========================================================
ruta_salida = os.path.join(BASE_DIR, "Dataset_Final")
os.makedirs(ruta_salida, exist_ok=True)

archivo_salida = os.path.join(ruta_salida, "dataset_unificado.csv")
df_final.to_csv(archivo_salida, index=False, encoding="utf-8-sig")

print(f"\nArchivo guardado como: {archivo_salida}")