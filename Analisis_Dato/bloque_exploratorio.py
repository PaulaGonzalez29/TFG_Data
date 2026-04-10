import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.stats import pearsonr
import os
import warnings
warnings.filterwarnings('ignore')

# 0. CARGA DE DATOS
# Crear carpetas de resultados si no existen
os.makedirs('Dataset_Final/ResultadosAnalisisExploratorio', exist_ok=True)
os.makedirs('Analisis_Dato/ResultadosAnalisisExploratorio', exist_ok=True)
# Dataset completo: para descriptivo y correlación
df = pd.read_csv('Dataset_Final/dataset_limpio_2023.csv')

# Dataset para PCA: variables con menos del 15% de nulos estructurales
df_pca = pd.read_csv('Dataset_Final/dataset_pca_2023.csv')

variables_todas = [c for c in df.columns if c not in ['country_code', 'year']]
variables_pca = [c for c in df_pca.columns if c not in ['country_code', 'year']]

print(f"Dataset completo — Países: {len(df)} | Variables: {len(variables_todas)}")
print(f"Dataset PCA     — Países: {len(df_pca)} | Variables: {len(variables_pca)}")
print(f"\nVariables excluidas del PCA por nulos estructurales (>15%):")
excluidas = [v for v in variables_todas if v not in variables_pca]
for v in excluidas:
    print(f"  - {v}")

# 1. ANÁLISIS DESCRIPTIVO
print("\n=== ESTADÍSTICAS DESCRIPTIVAS (dataset completo) ===")
print(df[variables_todas].describe().round(2).to_string())

# 2. ANÁLISIS DE CORRELACIÓN
corr_matrix = df[variables_todas].corr()

plt.figure(figsize=(16, 13))
sns.heatmap(
    corr_matrix,
    annot=True, fmt='.2f',
    cmap='coolwarm', center=0,
    square=True, linewidths=0.4,
    annot_kws={'size': 7}
)
plt.title('Matriz de Correlación entre Variables', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/correlacion.png', dpi=150)
plt.close()
print("\nGráfico de correlación guardado: correlacion.png")

# Correlaciones con electrificación total, ordenadas por valor absoluto
print("\n=== CORRELACIONES CON ACCESO TOTAL A ELECTRICIDAD ===")
corrs = []
for var in variables_todas:
    if var == 'elec_electricity_access_total':
        continue
    par = df[['elec_electricity_access_total', var]].dropna()
    if len(par) < 10:
        continue
    r, p = pearsonr(par['elec_electricity_access_total'], par[var])
    sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else ''
    corrs.append((var, r, sig))

corrs.sort(key=lambda x: abs(x[1]), reverse=True)
for var, r, sig in corrs:
    print(f"  {var:<45} r = {r:+.3f}  {sig}")

# Scatter: electrificación vs logro educativo
par_scatter = df[['elec_electricity_access_total', 'edu_educational_attainment_rate']].dropna()
plt.figure(figsize=(8, 6))
plt.scatter(
    par_scatter['elec_electricity_access_total'],
    par_scatter['edu_educational_attainment_rate'],
    alpha=0.6, color='steelblue', edgecolors='white', linewidth=0.5
)
m, b = np.polyfit(par_scatter['elec_electricity_access_total'],
                  par_scatter['edu_educational_attainment_rate'], 1)
x_line = np.linspace(par_scatter['elec_electricity_access_total'].min(),
                     par_scatter['elec_electricity_access_total'].max(), 100)
plt.plot(x_line, m * x_line + b, color='tomato', linewidth=2, label='Tendencia lineal')
plt.xlabel('Acceso a Electricidad Total (%)', fontsize=12)
plt.ylabel('Tasa de Logro Educativo (%)', fontsize=12)
plt.title('Electrificación vs Logro Educativo', fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/scatter_elec_attainment.png', dpi=150)
plt.close()
print("Gráfico scatter guardado: scatter_elec_attainment.png")

# 3. PCA — ANÁLISIS DE COMPONENTES PRINCIPALES
# A partir de aquí usamos df_pca: sin nulos, variables con cobertura suficiente
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_pca[variables_pca])

pca_full = PCA()
pca_full.fit(X_scaled)

varianza_acumulada = np.cumsum(pca_full.explained_variance_ratio_)
n_componentes = np.argmax(varianza_acumulada >= 0.80) + 1

print(f"\n=== PCA ({len(variables_pca)} variables, {len(df_pca)} países) ===")
print(f"Componentes necesarias para explicar el 80% de la varianza: {n_componentes}")
for i, (ind, acum) in enumerate(zip(pca_full.explained_variance_ratio_, varianza_acumulada)):
    print(f"  PC{i+1}: {ind:.3f} individual | {acum:.3f} acumulada")
    if acum >= 0.80:
        break

# Gráfico de varianza explicada acumulada
# Varianza explicada acumulada
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(range(1, len(varianza_acumulada) + 1),
        varianza_acumulada * 100,
        color='steelblue', marker='o', linewidth=2)

# Etiqueta de porcentaje en cada punto
for i, v in enumerate(varianza_acumulada):
    ax.annotate(f'{v*100:.1f}%',
                xy=(i + 1, v * 100),
                xytext=(0, 8),
                textcoords='offset points',
                ha='center', fontsize=8, color='steelblue')

# Línea del umbral 80%
ax.axhline(y=80, color='tomato', linestyle='--', linewidth=1.5, label='Umbral 80%')
ax.axvline(x=n_componentes, color='tomato', linestyle=':', linewidth=1.5,
           label=f'{n_componentes} componentes → 80% varianza')

ax.set_xlabel('Número de Componentes Principales', fontsize=12)
ax.set_ylabel('Varianza Explicada Acumulada (%)', fontsize=12)
ax.set_title('Varianza Explicada Acumulada — PCA', fontsize=14, fontweight='bold')
ax.set_ylim(0, 105)
ax.set_xticks(range(1, len(varianza_acumulada) + 1))
ax.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/pca_varianza_explicada.png', dpi=150)
plt.close()

# Ajuste final con las componentes seleccionadas
pca = PCA(n_components=n_componentes)
X_pca = pca.fit_transform(X_scaled)

# Cargas: qué variable tiene más peso en cada componente
loadings = pd.DataFrame(
    pca.components_.T,
    index=variables_pca,
    columns=[f'PC{i+1}' for i in range(n_componentes)]
)
print("\nCargas de las componentes principales:")
print(loadings.round(3).to_string())

# Heatmap de cargas
plt.figure(figsize=(12, 8))
sns.heatmap(
    loadings,
    annot=True, fmt='.2f',
    cmap='coolwarm', center=0,
    linewidths=0.5,
    annot_kws={'size': 8}
)
plt.title('Cargas de Componentes Principales (Loadings)', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/pca_loadings.png', dpi=150)
plt.close()
print("Heatmap de cargas guardado: pca_loadings.png")

# Guardar tabla de loadings como CSV
loadings.round(3).to_csv('Analisis_Dato/ResultadosAnalisisExploratorio/pca_loadings.csv', encoding='utf-8-sig', sep=';', decimal=',')
print("Tabla de loadings guardada: pca_loadings.csv")

# 4. CLUSTERIZACIÓN JERÁRQUICA
linkage_matrix = linkage(X_pca, method='ward')

plt.figure(figsize=(18, 7))
dendrogram(
    linkage_matrix,
    labels=df_pca['country_code'].values,
    leaf_rotation=90,
    leaf_font_size=6,
    color_threshold=0.7 * max(linkage_matrix[:, 2])
)
plt.title('Dendrograma — Clusterización Jerárquica', fontsize=14, fontweight='bold')
plt.xlabel('País', fontsize=11)
plt.ylabel('Distancia', fontsize=11)
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/dendrograma.png', dpi=150)
plt.close()
print("\nDendrograma guardado: dendrograma.png")

# 5. K-MEANS
SSE = []
rango_k = range(2, 16)

for k in rango_k:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X_pca)
    SSE.append(km.inertia_)

plt.figure(figsize=(8, 5))
plt.plot(rango_k, SSE, marker='o', color='steelblue', linewidth=2)
plt.xlabel('Número de Clusters (k)', fontsize=12)
plt.ylabel('SSE', fontsize=12)
plt.title('Método del Codo — K-Means', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/kmeans_codo.png', dpi=150)
plt.close()
print("Método del codo guardado: kmeans_codo.png")

# ─── Ajuste de k_optimo tras revisar el gráfico del codo ───
k_optimo = 4

km_final = KMeans(n_clusters=k_optimo, random_state=42, n_init=10)
df_pca['cluster'] = km_final.fit_predict(X_pca)

print(f"\n=== PERFIL DE LOS {k_optimo} CLUSTERS ===")
print(df_pca.groupby('cluster')[variables_pca].mean().round(2).T.to_string())

plt.figure(figsize=(9, 6))
colores = ['steelblue', 'tomato', 'seagreen', 'darkorange']
for c in range(k_optimo):
    mask = df_pca['cluster'] == c
    plt.scatter(X_pca[mask, 0], X_pca[mask, 1],
                label=f'Cluster {c}', alpha=0.7,
                color=colores[c], edgecolors='white', linewidth=0.5, s=60)
plt.xlabel('PC1', fontsize=12)
plt.ylabel('PC2', fontsize=12)
plt.title('Clusters de Países — K-Means (espacio PCA)', fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/kmeans_clusters.png', dpi=150)
plt.close()
print("Gráfico de clusters guardado: kmeans_clusters.png")

# 6. ANÁLISIS DE PERFILES DE CLUSTERS
# Variables clave para interpretar los clusters
# seleccionadas según su relevancia en PC1 y PC2
variables_perfil = [
    'elec_electricity_access_total',
    'elec_electricity_access_rural',
    'edu_educational_attainment_rate',
    'edu_primary_completion_rate',
    'edu_school_enrollment_secondary',
    'edu_out_of_school_children_female',
    'edu_out_of_school_children_male',
    'des_gdp_per_capita',
    'des_tasa_mortalidad_infantil',
    'des_tasa_natalidad',
]

# Perfil medio de cada cluster
perfil = df_pca.groupby('cluster')[variables_perfil].mean().round(2)
perfil.index = [f'Cluster {i}' for i in perfil.index]

print("\n=== PERFIL MEDIO DE CADA CLUSTER ===")
print(perfil.T.to_string())

perfil.T.to_csv(
    'Analisis_Dato/ResultadosAnalisisExploratorio/perfil_clusters.csv',
    encoding='utf-8-sig', sep=';', decimal=','
)
print("Tabla de perfiles guardada: perfil_clusters.csv")

# Países de cada cluster
print("\n=== PAÍSES POR CLUSTER ===")
for c in sorted(df_pca['cluster'].unique()):
    paises = df_pca[df_pca['cluster'] == c]['country_code'].tolist()
    print(f"\nCluster {c} ({len(paises)} países):")
    print(', '.join(paises))

# Guardar tabla de países por cluster
df_pca[['country_code', 'cluster']].sort_values(
    ['cluster', 'country_code']
).to_csv(
    'Analisis_Dato/ResultadosAnalisisExploratorio/paises_por_cluster.csv',
    index=False, encoding='utf-8-sig', sep=';'
)
print("\nTabla de países guardada: paises_por_cluster.csv")

# Gráfico de barras comparativo por variable y cluster
fig, axes = plt.subplots(2, 5, figsize=(18, 8))
axes = axes.flatten()
colores = ['steelblue', 'tomato', 'seagreen', 'darkorange']

for i, var in enumerate(variables_perfil):
    valores = [perfil.loc[f'Cluster {c}', var]
               for c in sorted(df_pca['cluster'].unique())]
    bars = axes[i].bar(
        [f'C{c}' for c in sorted(df_pca['cluster'].unique())],
        valores, color=colores
    )
    axes[i].set_title(var.replace('_', '\n'), fontsize=8, fontweight='bold')
    for bar, val in zip(bars, valores):
        axes[i].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(valores) * 0.02,
            f'{val:.1f}', ha='center', va='bottom', fontsize=7
        )

plt.suptitle('Perfil Comparativo de Clusters', fontsize=14,
             fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/perfil_clusters.png',
            dpi=150, bbox_inches='tight')
plt.close()
print("Gráfico de perfiles guardado: perfil_clusters.png")

# Scatter electrificación vs logro educativo coloreado por cluster
par = df_pca[['elec_electricity_access_total',
              'edu_educational_attainment_rate',
              'cluster']].copy()

plt.figure(figsize=(9, 6))
for c in sorted(df_pca['cluster'].unique()):
    mask = par['cluster'] == c
    plt.scatter(
        par.loc[mask, 'elec_electricity_access_total'],
        par.loc[mask, 'edu_educational_attainment_rate'],
        label=f'Cluster {c}', alpha=0.7,
        color=colores[c], edgecolors='white', linewidth=0.5, s=60
    )
plt.xlabel('Acceso a Electricidad Total (%)', fontsize=12)
plt.ylabel('Tasa de Logro Educativo (%)', fontsize=12)
plt.title('Electrificación vs Logro Educativo por Cluster',
          fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisExploratorio/scatter_clusters.png', dpi=150)
plt.close()
print("Scatter por clusters guardado: scatter_clusters.png")

print("\nBloque exploratorio completado.")