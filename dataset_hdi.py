import pandas as pd
import unicodedata

# =========================
# RUTAS DE ENTRADA / SALIDA
# =========================
INPUT_HDI = "Datos TFG/Desarrollo/Human Develop Index HDI.csv"
INPUT_PAISES = "Datos TFG/Listado_paises.csv"
OUTPUT_CSV = "Datos TFG/Desarrollo/hdi_limpio.csv"

# =========================
# FUNCIONES AUXILIARES
# =========================
def normalizar_texto(texto: str) -> str:
    """
    Normaliza nombres de países para poder hacer el cruce aunque haya
    tildes, mayúsculas/minúsculas o pequeños cambios de formato.
    """
    if pd.isna(texto):
        return ""
    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    texto = " ".join(texto.split())
    return texto

def convertir_valor(valor):
    """
    Convierte valores tipo '0,841' a float 0.841.
    Devuelve None si está vacío.
    """
    if pd.isna(valor):
        return None
    valor = str(valor).strip()
    if valor == "":
        return None
    valor = valor.replace(",", ".")
    try:
        return float(valor)
    except ValueError:
        return None

# =========================
# 1) LEER LISTADO DE PAÍSES
# =========================
paises = pd.read_csv(INPUT_PAISES, sep=";", encoding="utf-8")

# Comprobación básica
columnas_esperadas_paises = {"name", "nombre", "country_code"}
faltan = columnas_esperadas_paises - set(paises.columns)
if faltan:
    raise ValueError(f"En {INPUT_PAISES} faltan columnas obligatorias: {faltan}")

# =========================
# 2) LEER ARCHIVO HDI EN BRUTO
# =========================
# Este CSV tiene varias filas de cabecera, por eso se lee sin header
hdi_raw = pd.read_csv(INPUT_HDI, sep=";", header=None, encoding="utf-8")

# La fila 4 contiene los años correctos:
# col 1 = Country
# col 2 = 1990
# col 4 = 2000
# col 6 = 2010
# col 8 = 2015
# col 10 = 2020
# col 12 = 2021
# col 14 = 2022
# col 16 = 2023
columnas_hdi = [1, 2, 4, 6, 8, 10, 12, 14, 16]
nombres_columnas = ["country", "1990", "2000", "2010", "2015", "2020", "2021", "2022", "2023"]

hdi = hdi_raw.iloc[6:, columnas_hdi].copy()
hdi.columns = nombres_columnas

# Quitar filas vacías
hdi = hdi[hdi["country"].notna()].copy()

# Convertir los valores numéricos
anios = ["1990", "2000", "2010", "2015", "2020", "2021", "2022", "2023"]
for col in anios:
    hdi[col] = hdi[col].apply(convertir_valor)

# Conservar solo filas que tengan al menos un valor HDI
hdi = hdi[hdi[anios].notna().any(axis=1)].copy()

# =========================
# 3) MAPEO DE NOMBRES ENTRE HDI Y LISTADO_PAISES
# =========================
# Aquí se resuelven diferencias de nombres entre ambos archivos
alias_hdi_a_listado = {
    "Bolivia (Plurinational State of)": "Bolivia",
    "Brunei Darussalam": "Brunei",
    "Eswatini (Kingdom of)": "Eswatini",
    "Hong Kong, China (SAR)": "Hong Kong",
    "Iran (Islamic Republic of)": "Iran",
    "Côte d'Ivoire": "Ivory Coast",
    "Korea (Democratic People's Rep. of)": "North Korea",
    "Korea (Republic of)": "South Korea",
}

# Crear nombre de cruce para HDI
hdi["country_match"] = hdi["country"].replace(alias_hdi_a_listado)
hdi["country_norm"] = hdi["country_match"].apply(normalizar_texto)

# Crear nombre de cruce para listado_paises
paises["country_norm"] = paises["name"].apply(normalizar_texto)

# =========================
# 4) UNIR CON ISO CODE
# =========================
hdi = hdi.merge(
    paises[["country_norm", "country_code"]],
    on="country_norm",
    how="left"
)

# Nos quedamos solo con países que existan en listado_paises
# (esto elimina regiones/agregados como "World", "Arab States", etc.)
hdi = hdi[hdi["country_code"].notna()].copy()

# Seguridad extra: no puede quedar ningún código ISO en blanco
if hdi["country_code"].isna().any() or (hdi["country_code"].astype(str).str.strip() == "").any():
    filas_problematicas = hdi[hdi["country_code"].isna() | (hdi["country_code"].astype(str).str.strip() == "")]
    raise ValueError(
        "Hay países sin código ISO después del cruce. Revisa estos países:\n"
        + filas_problematicas["country"].to_string(index=False)
    )

# =========================
# 5) PASAR A FORMATO LARGO
# =========================
resultado = hdi.melt(
    id_vars=["country_code"],
    value_vars=anios,
    var_name="year",
    value_name="value"
)

# Quitar años sin valor
resultado = resultado[resultado["value"].notna()].copy()

# Renombrar columnas finales
resultado = resultado.rename(columns={"country_code": "iso_code"})

# Orden final
resultado["year"] = resultado["year"].astype(int)
resultado = resultado.sort_values(["iso_code", "year"]).reset_index(drop=True)

# =========================
# 6) GUARDAR CSV FINAL
# =========================
resultado.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

print(f"CSV generado correctamente: {OUTPUT_CSV}")
print(resultado.head(20))

# =========================
# 7) INFORMACIÓN ÚTIL OPCIONAL
# =========================
# Países del listado que no aparecen en el HDI
paises_hdi_norm = set(hdi["country_norm"].unique())
faltan_en_hdi = paises[~paises["country_norm"].isin(paises_hdi_norm)][["name", "country_code"]]

if not faltan_en_hdi.empty:
    print("\nPaíses que están en Listado_paises pero no aparecen en el archivo HDI:")
    print(faltan_en_hdi.to_string(index=False))