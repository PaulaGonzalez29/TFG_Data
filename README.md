# ELECTRIFICACIÓN Y OPORTUNIDADES EDUCATIVAS: El impacto del acceso a electricidad en el desarrollo social
### Trabajo de Fin de Grado — Business Analytics | Universidad Francisco de Vitoria
### PAULA GONZÁLEZ RODRÍGUEZ

Análisis del impacto del acceso a la electricidad en el desarrollo educativo 
a escala global, utilizando datos de 205 países para el año 2023.

## Estructura del repositorio

### Ingeniería del dato
| Script | Descripción |
|--------|-------------|
| `base_datos_educacion.py` | Extracción e integración de variables educativas |
| `base_datos_Electrif.py` | Extracción e integración de variables de electrificación |
| `base_datos_desarrollo.py` | Extracción e integración de variables de desarrollo |
| `dataset_hdi.py` | Limpieza y normalización del dataset HDI (PNUD) |
| `unificar_dataset.py` | Integración global de los tres bloques temáticos |
| `dataset_paises.py` | Filtrado al año 2023 |
| `analisis_calidad.py` | Análisis de calidad antes y después de la limpieza |
| `limpieza_datos.py` | Limpieza, imputación y preparación del dataset final |
| `eda.py` | Análisis Exploratorio de Datos |

### Análisis del dato (`Analisis_Dato/`)
| Script | Descripción |
|--------|-------------|
| `bloque_exploratorio.py` | PCA, K-Means y clusterización jerárquica |
| `bloque_predictivo_regresion.py` | Regresión lineal y Random Forest |

### Datasets (`Dataset_Final/`)
| Archivo | Descripción |
|---------|-------------|
| `dataset_unificado.csv` | Dataset histórico completo |
| `dataset_transversal_2023.csv` | Corte transversal filtrado a 2023 |
| `dataset_limpio_2023.csv` | Dataset final tras limpieza e imputación |
| `dataset_pca_2023.csv` | Dataset preparado para PCA |

### Outputs
| Carpeta | Descripción |
|---------|-------------|
| `Analisis_Calidad/` | Resúmenes de calidad antes y después de la limpieza |
| `Analisis_Dato/ResultadosAnalisisExploratorio/` | Gráficos y tablas del bloque exploratorio |
| `Analisis_Dato/ResultadosAnalisisPredictivo/` | Métricas y gráficos del bloque predictivo |

## Orden de ejecución

Los scripts deben ejecutarse en el siguiente orden:

1. `base_datos_educacion.py`
2. `base_datos_Electrif.py`
3. `dataset_hdi.py`
4. `base_datos_desarrollo.py`
5. `unificar_dataset.py`
6. `dataset_paises.py`
7. `analisis_calidad.py` *(antes de la limpieza)*
8. `limpieza_datos.py`
9. `analisis_calidad.py` *(después de la limpieza)*
10. `eda.py`
11. `Analisis_Dato/bloque_exploratorio.py`
12. `Analisis_Dato/bloque_predictivo_regresion.py`
