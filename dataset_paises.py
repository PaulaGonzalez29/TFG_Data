import pandas as pd
import os

# Carpeta donde está este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Posibles ubicaciones de los archivos
RUTAS_POSIBLES = [
    BASE_DIR,
    os.path.join(BASE_DIR, "Datos TFG")
]

# Archivos unificados de entrada
archivos_datasets = {
    "dataset_educacion_unificado.csv": "educacion",
    "dataset_desarrollo_unificado.csv": "desarrollo",
    "dataset_electrificacion_unificado.csv": "electrificacion"  # así aparece en tu carpeta
}

def buscar_archivo(nombre_archivo):
    """Busca un archivo en las rutas posibles."""
    for ruta_base in RUTAS_POSIBLES:
        ruta_completa = os.path.join(ruta_base, nombre_archivo)
        if os.path.exists(ruta_completa):
            return ruta_completa
    return None

def leer_dataset_2023(ruta_archivo, nombre_dataset):
    """Carga un dataset, normaliza columnas y se queda solo con 2023."""
    df = pd.read_csv(ruta_archivo)

    # Normalizar nombres de columnas
    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    # Renombrar columnas si hiciera falta
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
    df["country_code"] = df["country_code"].astype(str).str.strip()
    df = df[df["country_code"].notna()]
    df = df[df["country_code"] != ""]
    df = df[df["country_code"].str.lower() != "nan"]

    # Convertir year a numérico
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Filtrar solo 2023
    df = df[df["year"] == 2023].copy()

    if df.empty:
        raise ValueError(f"El dataset {nombre_dataset} no tiene datos para 2023.")

    # Eliminar columnas de texto que puedan duplicar información
    columnas_eliminar = ["Country Name", "country_name", "dataset", "fuente"]
    df = df.drop(columns=columnas_eliminar, errors="ignore")

    # No incluir columnas vacías en 2023
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
        print(f"\nColumnas no incluidas en {nombre_dataset} por estar vacías en 2023:")
        for col in columnas_excluidas:
            print(f"- {col}")

    # Eliminar duplicados por país si existen
    df = df.drop_duplicates(subset=["country_code"], keep="first")

    return df

lista_dfs = []

for archivo, nombre_dataset in archivos_datasets.items():
    ruta_archivo = buscar_archivo(archivo)

    if ruta_archivo is None:
        print(f"No encontrado: {archivo}")
        continue

    try:
        df = leer_dataset_2023(ruta_archivo, nombre_dataset)

        # Prefijo a las variables para distinguir bloques
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

        lista_dfs.append(df)
        print(f"OK: {archivo} -> {ruta_archivo}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")

# Unir todos los datasets por country_code y year
df_final = lista_dfs[0]

for df in lista_dfs[1:]:
    df_final = df_final.merge(df, on=["country_code", "year"], how="outer")

# Eliminar filas sin country_code
df_final = df_final.dropna(subset=["country_code"])
df_final["country_code"] = df_final["country_code"].astype(str).str.strip()
df_final = df_final[df_final["country_code"] != ""]
df_final = df_final[df_final["country_code"].str.lower() != "nan"]

# Eliminar duplicados finales
df_final = df_final.drop_duplicates(subset=["country_code"], keep="first")

# Ordenar
df_final = df_final.sort_values("country_code").reset_index(drop=True)

# Revisar resultado
print("\nPrimeras filas:")
print(df_final.head())

print("\nInformación general:")
print(df_final.info())

print("\nDimensión final:")
print(df_final.shape)

print("\nPorcentaje de nulos por columna:")
print((df_final.isnull().mean() * 100).round(2).sort_values(ascending=False))

# Guardar dataset final
ruta_salida = os.path.join(BASE_DIR, "Dataset_Final")
os.makedirs(ruta_salida, exist_ok=True)

archivo_salida = os.path.join(ruta_salida, "dataset_transversal_2023.csv")
df_final.to_csv(archivo_salida, index=False)

print(f"\nArchivo guardado como: {archivo_salida}")