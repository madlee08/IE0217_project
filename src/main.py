import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt


class importar_arxiv:
    def __init__(self, ruta_de_archivo):
        if not ruta_de_archivo.endswith(self.extension):
            raise Exception('Formato incorrecto')
        self.ruta = ruta_de_archivo

class importar_csv(importar_arxiv):
    extension = 'csv'

    def importar(self):
        return pd.read_csv(self.ruta)

class importar_arxivs:
    def __init__(self, ruta_de_archivo):
        valido = False
        for extension in self.extensiones:
            if ruta_de_archivo.endswith(extension):
                valido = True

        if not valido:
            raise Exception('Formato incorrecto')
        self.ruta = ruta_de_archivo

class importar_excel(importar_arxivs):
    extensiones = ['xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'odt']

    def importar(self, hoja=0):
        return pd.read_excel(self.ruta, sheet_name=hoja)

class importar_geodata(importar_arxivs):
    extensiones = ['shp', 'gpkg', 'geojson']

    def importar(self):
        return gpd.read_file(self.ruta)

data = importar_csv('../Datos/06_30_21_CSV_ACTIVOS.csv')
DF = data.importar()
print(DF)

geodata = importar_geodata('../Datos/costa_rica_cantones.geojson')
GDF = geodata.importar()
print(GDF)

data_excel = importar_excel('../Datos/07_12_21_EXCEL_SERIES.xlsx')
DF_excel = data_excel.importar(hoja='2_5 CANT_NUEVOS')
print(DF_excel)
