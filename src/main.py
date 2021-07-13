import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt


class importar_arxiv_pd:
    def __init__(self, ruta_de_archivo):
        if not ruta_de_archivo.endswith(self.extension):
            raise Exception('Formato incorrecto')
        self.ruta = ruta_de_archivo

class importar_xlsx(importar_arxiv_pd):
    extension = 'xlsx'

    def importar(self):
        return pd.read_excel(self.ruta)

class importar_csv(importar_arxiv_pd):
    extension = 'csv'

    def importar(self):
        return pd.read_csv(self.ruta)

class importar_arxiv_gpd:
    def __init__(self, ruta_de_archivo):
        valido = False
        for extension in self.extensiones:
            if ruta_de_archivo.endswith(extension):
                valido = True

        if not valido:
            raise Exception('Formato incorrecto')
        self.ruta = ruta_de_archivo

class importar_geodata(importar_arxiv_gpd):
    extensiones = ['shp', 'gpkg', 'geojson']

    def importar(self):
        return gpd.read_file(self.ruta)

data = importar_csv('../Datos/06_30_21_CSV_ACTIVOS.csv')
DF = data.importar()
print(DF)

geodata = importar_geodata('../Datos/costa_rica_cantones.geojson')
GDF = geodata.importar()
print(GDF)