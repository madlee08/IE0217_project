import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt


class importar_arxivs:
    def __init__(self, ruta_de_archivo):
        valido = False
        for extension in self.extensiones:
            if ruta_de_archivo.endswith(extension):
                valido = True

        if not valido:
            raise Exception('Formato incorrecto')
        self.ruta = ruta_de_archivo

class importar_csv(importar_arxivs):
    extensiones = ['csv']

    def importar(self):
        return pd.read_csv(self.ruta)

class importar_excel(importar_arxivs):
    extensiones = ['xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'odt']

    def importar(self, hoja=0):
        return pd.read_excel(self.ruta, sheet_name=hoja)

class importar_geodata(importar_arxivs):
    extensiones = ['shp', 'gpkg', 'geojson']

    def importar(self):
        return gpd.read_file(self.ruta)

class importar_a_pd:
    def excel(self, ruta_de_archivo, hoja=0):
        arxiv_excel = importar_excel(ruta_de_archivo)
        return arxiv_excel.importar(hoja=hoja)
    
    def csv(self, ruta_de_archivo):
        arxiv_csv = importar_csv(ruta_de_archivo)
        return arxiv_csv.importar()
    
    def geodata(self, ruta_de_archivo):
        arvix_geojson = importar_geodata(ruta_de_archivo)
        return arvix_geojson.importar()


class DFs_base:
    lista_df = dict()

class DFs_procesar(DFs_base):
    importar = importar_a_pd()

    def agregar_excel(self, df_nombre_nuevo, ruta_de_archivo, hoja=0):
        self.lista_df[df_nombre_nuevo] = self.importar.excel(ruta_de_archivo, hoja=hoja)
    
    def agregar_csv(self, df_nombre_nuevo, ruta_de_archivo):
        self.lista_df[df_nombre_nuevo] = self.importar.csv(ruta_de_archivo)
    
    def agregar_geodata(self, gdf_nombre_nuevo, ruta_de_archivo):
        self.lista_df[gdf_nombre_nuevo] = self.importar.geodata(ruta_de_archivo)

    def remover_indices(self, df_nombre, lista_de_indices):
        self.lista_df[df_nombre].drop(lista_de_indices, inplace=True)

    def remover_columnas(self, df_nombre, lista_de_columnas):
        self.lista_df[df_nombre].drop(lista_de_columnas, inplace=True, axis=1)

    def renombrar_columnas(self, df_nombre, lista_de_columnas):
        self.lista_df[df_nombre].rename(columns=lista_de_columnas, inplace=True)

    def fusionar_gdf_df(self, gdf_nombre_nuevo, gdf_nombre, df_nombre, pivote):
        self.lista_df[gdf_nombre_nuevo] = self.lista_df[gdf_nombre].merge(self.lista_df[df_nombre], on=pivote)

    def agregar_columna(self, df_nombre, nombre_columna_nuevo, valores):
        self.lista_df[df_nombre][nombre_columna_nuevo] = valores
    
    def fijar_indice(self, df_nombre, nombre_columna):
        self.lista_df[df_nombre].set_index(nombre_columna, inplace=True)


class DFs (DFs_procesar):
    def main(self):
        self.agregar_datos()
        self.limpiar_datos()
        print(self.lista_df['cantones'])
        print(self.lista_df['activos'])

    def agregar_datos(self):
        self.agregar_geodata('cantones', '../Datos/costa_rica_cantones.geojson')
        self.agregar_csv('activos', '../Datos/06_30_21_CSV_ACTIVOS.csv')

    def limpiar_datos(self):
        self.remover_columnas('cantones', ['osm_id', 'boundary', 'admin_level', 'parents', 'name', 'name_en'])
        self.renombrar_columnas('cantones', {'local_name':'canton'})
        self.fijar_indice('cantones', 'canton')
        self.lista_df['cantones'].index = self.lista_df['cantones'].index.str.replace('Cantón ', '')

        self.remover_columnas('activos', ['cod_provin', 'provincia', 'cod_canton'])
        self.fijar_indice('activos', 'canton')

Data = DFs()
Data.main()