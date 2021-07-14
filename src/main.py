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
    lista_df_limpio = dict()

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

    def remover_columnas(self, df_nombre, lista_de_columnas, invertido=False):
        if not invertido:
            self.lista_df[df_nombre].drop(lista_de_columnas, inplace=True, axis=1)
        else:
            self.lista_df[df_nombre] = self.lista_df[df_nombre][self.lista_df[df_nombre].columns.intersection(lista_de_columnas)]
    
    def renombrar_columnas(self, df_nombre, lista_de_columnas):
        self.lista_df[df_nombre].rename(columns=lista_de_columnas, inplace=True)

    def fusionar_dfs(self, df_nombre_nuevo, df_nombre1, df_nombre2, pivote):
        self.lista_df_limpio[df_nombre_nuevo] = self.lista_df[gdf_nombre1].merge(self.lista_df[df_nombre2], on=pivote)

    def agregar_columna(self, df_nombre, nombre_columna_nuevo, valores):
        self.lista_df[df_nombre][nombre_columna_nuevo] = valores
    
    def fijar_indice(self, df_nombre, nombre_columna):
        self.lista_df[df_nombre].set_index(nombre_columna, inplace=True)

    def columnas_a_datetime(self, df_nombre):
        columnas = self.lista_df[df_nombre].columns
        self.lista_df[df_nombre].columns = pd.to_datetime(columnas, infer_datetime_format=True)
    
    def devolver_columnas(self, df_nombre):
        return self.lista_df[df_nombre_nuevo].columns.to_list()

    def imprimir_df(self):
        for df in self.lista_df.keys():
            print(self.lista_df[df])

from datetime import date, datetime, timedelta

class formato_fechas:
    def __init__(self, fecha_inicial, fecha_final, fecha_archivo):
        self.fecha_max = self.datetime_max(fecha_archivo)
        self.fecha_min = date(2020, 4, 21)

        if fecha_inicial.isnumeric() and not fecha_final.isnumeric():
            self.fin = self.str_a_datetime_final(fecha_final)
            self.inicio = self.str_a_datetime_inicial(fecha_inicial)

        else:
            self.inicio = self.str_a_datetime_inicial(fecha_inicial)
            self.fin = self.str_a_datetime_final(fecha_final)
        
        self.lista_dias = self.rango_dias(self.inicio, self.fin)

    def datetime_max(self, fecha_max):
        if fecha_max == '':
            hoy = datetime.now()
            hoy20horas = hoy.replace(hour=20, minute=0, second=0, microsecond=0)
            
            if hoy < hoy20horas:
                return date.today() + timedelta(days=-1)
            else:
                return date.today()
        else:
            dia, mes, anio = fecha_max.split('/')
            return date(int(anio), int(mes), int(dia))

    def str_a_datetime_inicial(self, fecha_inicial):
        if fecha_inicial !='' and not fecha_inicial.isnumeric():
            dia, mes, anio = fecha_inicial.split('/')
            fecha_inicio = date(int(anio), int(mes), int(dia))
            
            if fecha_inicio < self.fecha_min:
                fecha_inicio = self.fecha_min
            return fecha_inicio

        elif fecha_inicial =='':
            return self.fecha_max
        
        elif fecha_inicial.isnumeric():
            fecha_inicio = self.fin + timedelta(days=-int(fecha_inicial))
            
            if fecha_inicio < self.fecha_min:
                fecha_inicio = self.fecha_min
            return fecha_inicio

    def str_a_datetime_final(self, fecha_final):
        if fecha_final !='' and not fecha_final.isnumeric():
            dia, mes, anio = fecha_final.split('/')
            fecha_fin = date(int(anio), int(mes), int(dia))
            
            if fecha_fin > self.fecha_max:
                fecha_fin = self.fecha_max
            return fecha_fin

        elif fecha_final =='':
            return self.fecha_max
        
        elif fecha_final.isnumeric():
            fecha_fin =  self.inicio + timedelta(days=int(fecha_final))

            if fecha_fin > self.fecha_max:
                fecha_fin = self.fecha_max
            return fecha_fin

    def rango_dias(self, fecha_inicial, fecha_final):
        return pd.date_range(start=fecha_inicial, end=fecha_final).to_list()

    def get_fecha_max(self):
        return self.fecha_max
    
    def get_fecha_inicial(self):
        return self.inicio
    
    def get_fecha_final(self):
        return self.fin

    def get_lista_dias(self):
        return self.lista_dias

class DFs (DFs_procesar):
    def __init__(self, fecha_inicial='', fecha_final='', fecha_archivo='', indicadores=[]):
        fecha = formato_fechas(fecha_inicial, fecha_final, fecha_archivo)

        self.fecha_max = fecha.get_fecha_max()
        self.fecha_inicial = fecha.get_fecha_inicial()
        self.fecha_final = fecha.get_fecha_final()
        self.lista_dias = fecha.get_lista_dias()

        self.indicadores = indicadores

    def main(self):
        self.agregar_datos()
        self.limpiar_datos()
        self.imprimir_df()
        print(self.inicio)
        print(self.fin)
        print(self.fecha_max)
        print(self.lista_dias)

    def agregar_datos(self):
        self.agregar_geodata('cantones', '../Datos/costa_rica_cantones.geojson')
        self.agregar_csv('nombres', '../Datos/costa_rica_cantones_nombres.csv')

        if not self.indicadores:
            self.indicadores = ['positivos', 'activos', 'recup', 'fallecidos']

        anio = str(self.fecha_max.year)
        anio_corto = str(self.fecha_max.year - 2000)

        if self.fecha_max.month in range(1, 10):
            mes = '0' + str(self.fecha_max.month)
        else:
            mes = str(self.fecha_max.month)
        
        if self.fecha_max.day in range(1, 10):
            dia = '0' + str(self.fecha_max.day)
        else:
            dia = str(self.fecha_max.day) 

        for indicador in self.indicadores:
            self.agregar_csv(indicador, '../Datos/{}_{}_{}_CSV_{}.csv'.format(mes, dia, anio_corto, indicador.upper()))

    def limpiar_datos(self):
        self.remover_columnas('cantones', ['local_name', 'geometry'], invertido=True)
        self.renombrar_columnas('cantones', {'local_name':'canton'})
        self.fijar_indice('cantones', 'canton')
        self.lista_df['cantones'].index = self.lista_df['cantones'].index.str.replace('Cantón ', '')
        
        for indicador in self.indicadores:
            self.remover_columnas(indicador, ['cod_provin', 'provincia', 'cod_canton'])
            self.lista_df[indicador].canton = self.lista_df['nombres']
            self.fijar_indice(indicador, 'canton')
            self.columnas_a_datetime(indicador)
            self.remover_columnas(indicador, self.lista_dias, invertido=True)

Data = DFs('2', '')
Data.main()