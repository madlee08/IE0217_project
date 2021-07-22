import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta
from matplotlib.backends.backend_pdf import PdfPages

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

class DFs_procesar (DFs_base):
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
        self.lista_df_limpio[df_nombre_nuevo] = self.lista_df[df_nombre1].merge(self.lista_df[df_nombre2], on=pivote)

    def agregar_columna(self, df_nombre, nombre_columna_nuevo, valores):
        self.lista_df[df_nombre][nombre_columna_nuevo] = valores
    
    def fijar_indice(self, df_nombre, nombre_columna):
        self.lista_df[df_nombre].set_index(nombre_columna, inplace=True)

    def columnas_a_datetime(self, df_nombre):
        columnas = self.lista_df[df_nombre].columns
        self.lista_df[df_nombre].columns = pd.to_datetime(columnas, infer_datetime_format=True)
    
    def devolver_columnas(self, df_nombre):
        return self.lista_df[df_nombre].columns.to_list()

    def imprimir_df(self):
        for df in self.lista_df.keys():
            print(self.lista_df[df])

    def imprimir_df_limpio(self):
        for df in self.lista_df_limpio.keys():
            print(self.lista_df_limpio[df])

    def agregar_diff_columna(self, df_nombre, nombre_columna_nuevo, fecha_inicial, fecha_final):
        columnas = self.devolver_columnas(df_nombre)

        primero = columnas[0]
        ultimo = columnas[-1]

        if fecha_inicial != fecha_final:
            diff = self.lista_df[df_nombre][ultimo] - self.lista_df[df_nombre][primero]
        else:
            diff = self.lista_df[df_nombre][primero]
        
        self.agregar_columna(df_nombre, nombre_columna_nuevo, diff)

    def agregar_media_columna(self, df_nombre, nombre_columna_nuevo):
        media = self.lista_df[df_nombre].mean(axis=1)
        self.agregar_columna(df_nombre, nombre_columna_nuevo, media) 

class DFs_graficar(DFs_base):
    output_archivo = str()
    lista_fig = list()

    def graficar_df(self, df_nombre, columna, color, fecha_inicial, fecha_final):
        drop, indicador = df_nombre.split('-')
        fig, (ax1, ax2) = plt.subplots(1,2, figsize=(8,4))

        leyenda = str()

        if fecha_inicial == fecha_final:
            self.output_archivo = 'Reporte_{}'.format(fecha_inicial)

            leyenda = 'Casos {} por canton'.format(indicador)
            fig.suptitle(leyenda + '\npara el dia {}'.format(fecha_inicial))

        else:
            self.output_archivo = 'Reporte_{}_{}'.format(fecha_inicial,fecha_final)

            if indicador == 'activos' or indicador == 'nuevos':
                leyenda = 'Casos {} promedio por canton'.format(indicador)
                fig.suptitle(leyenda + '\ndesde {} hasta {}'.format(fecha_inicial, fecha_final))

            else:
                leyenda = 'Diferencia de casos {} por canton'.format(indicador)
                fig.suptitle(leyenda + '\ndesde {} hasta {}'.format(fecha_inicial, fecha_final))                

        self.lista_df_limpio[df_nombre].plot(columna, cmap=color, edgecolor ='k', linewidth = 0.5, ax=ax1)
        ax1.set_xlim(-86.1, -82.5)
        ax1.set_ylim(8, 11.3)
        ax1.set_xticks([])
        ax1.set_yticks([])
        ax1.set_title('Vista general')

        self.lista_df_limpio[df_nombre].plot(columna, cmap=color, edgecolor ='k', linewidth = 0.5, ax=ax2, legend=True, legend_kwds={'label':leyenda, 'orientation':'vertical'})
        ax2.set_xlim(-84.4, -83.7)
        ax2.set_ylim(9.7, 10.3)
        ax2.set_xticks([])
        ax2.set_yticks([])
        ax2.set_title('Cantones de la GAM')

        self.lista_fig.append(fig)
    
    def crear_pdf(self, nombre_archivo=''):
        if nombre_archivo =='':
            nombre_archivo = self.output_archivo

        with PdfPages('../output/'+nombre_archivo+'.pdf') as pdf:
            for fig in self.lista_fig:
                pdf.savefig(fig)

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

class get_str_fecha:
    def __init__(self, fecha_datetime):
        self.fecha = fecha_datetime

    def get_dia_str(self):
        dia = str()

        if self.fecha.day in range(1, 10):
            dia = '0' + str(self.fecha.day)

        else:
            dia = str(self.fecha.day)
        
        return dia

    def get_mes_str(self):
        mes = str()

        if self.fecha.month in range(1, 10):
            mes = '0' + str(self.fecha.month)

        else:
            mes = str(self.fecha.month)

        return mes
    
    def get_anio_str(self, corto=False):
        anio = str()

        if corto:
            anio = str(self.fecha.year - 2000)
        
        else:
            anio = str(self.fecha.year)

        return anio

class DFs (DFs_procesar, DFs_graficar):
    def __init__(self, fecha_inicial='', fecha_final='', fecha_archivo='', indicadores=['positivos', 'activos', 'recuperados', 'fallecidos'], mostrar_graficas=False, verboso=False):
        fecha = formato_fechas(fecha_inicial, fecha_final, fecha_archivo)

        self.fecha_max = fecha.get_fecha_max()
        self.fecha_inicial = fecha.get_fecha_inicial()
        self.fecha_final = fecha.get_fecha_final()
        self.lista_dias = fecha.get_lista_dias()

        self.indicadores = indicadores
        self.mostrar_graficas = mostrar_graficas
        self.verboso = verboso

    def main(self):
        self.agregar_datos()
        self.limpiar_datos()
        self.fusionar_datos()
        self.graficar_datos()
        self.pdf()
        if self.mostrar_graficas:
            plt.show()
        plt.close()

    def agregar_datos(self):
        self.agregar_geodata('cantones', '../Datos/costa_rica_cantones.geojson')

        if self.verboso:
            print('='*70)
            print('GEODATAFRAME AGREGADO')
            print('='*70)
            print(self.lista_df['cantones'])
            print('='*70)
            print()

        fecha = get_str_fecha(self.fecha_max)

        anio_corto = fecha.get_anio_str(corto=True)
        anio = fecha.get_anio_str()
        mes = fecha.get_mes_str()
        dia = fecha.get_dia_str()

        hojas = {'positivos': '2_1CANT_ACUMULADOS', 'recuperados': '2_2 CANT_RECUPERADOS', 'fallecidos': '2_3 CANT_FALLECIDOS', 'activos': '2_4 CANT_ACTIVOS'}
        enlace = 'http://geovision.uned.ac.cr/oges/archivos_covid/{}_{}_{}/{}_{}_{}_EXCEL_SERIES.xlsx'.format(anio, mes, dia, mes, dia, anio_corto)

        for indicador in self.indicadores:
            self.agregar_excel(indicador, enlace, hoja=hojas[indicador])

            if self.verboso:
                print('='*70)
                print('DATAFRAME DEL INDICADOR {} AGREGADO'.format(indicador))
                print('='*70)
                print(self.lista_df[indicador])
                print('='*70)
                print()

    def limpiar_datos(self):
        if self.verboso:
            print('='*70)
            print('MODIFICANDO GEODATAFRAME')
            print('='*70)
            print()

        self.remover_columnas('cantones', ['local_name', 'geometry'], invertido=True)

        if self.verboso:
            print(self.lista_df['cantones'])
            print('='*70)

        self.renombrar_columnas('cantones', {'local_name':'canton'})

        if self.verboso:
            print(self.lista_df['cantones'])
            print('='*70)

        self.fijar_indice('cantones', 'canton')
        self.lista_df['cantones'].index = self.lista_df['cantones'].index.str.replace('Cantón ', '')


        if self.verboso:
            print(self.lista_df['cantones'])
            print('='*70)
        
        if self.verboso:
            print('='*70)
            print('MODIFICACION DEL GEODATAFRAME FINALIZADO')
            print('='*70)
            print()

        for indicador in self.indicadores:
            if self.verboso:
                print('='*70)
                print('MODIFICANDO DATAFRAME DEL INDICADOR {}'.format(indicador))
                print('='*70)
                print()

            self.remover_columnas(indicador, ['cod_provin', 'provincia', 'cod_canton'])

            if self.verboso:
                print(self.lista_df[indicador])
                print('='*70)

            self.fijar_indice(indicador, 'canton')
            self.columnas_a_datetime(indicador)

            if self.verboso:
                print(self.lista_df[indicador])
                print('='*70)

            self.remover_columnas(indicador, self.lista_dias, invertido=True)

            if self.verboso:
                print(self.lista_df[indicador])
                print('='*70)

            if indicador == 'activos' or indicador == 'nuevos':
                self.agregar_media_columna(indicador, 'DATA')

            else:
                self.agregar_diff_columna(indicador, 'DATA', self.fecha_inicial, self.fecha_final)

            if self.verboso:
                print(self.lista_df[indicador])
                print('='*70)

            self.remover_columnas(indicador, ['DATA'], invertido=True)

            if self.verboso:
                print(self.lista_df[indicador])
                print('='*70)

            if self.verboso:
                print('='*70)
                print('MODIFICACION DEL DATAFRAME DEL INDICADOR {} FINALIZADO'.format(indicador))
                print('='*70)
                print()

    def fusionar_datos(self):
        for indicador in self.indicadores:
            df_nombre = 'cantones-' + indicador

            self.fusionar_dfs(df_nombre, 'cantones', indicador, pivote='canton')

            if self.verboso:
                print('='*70)
                print('DATAFRAME FUSIONADO ENTRE CANTONES Y {}'.format(indicador))
                print('='*70)
                print(self.lista_df_limpio[df_nombre])
                print('='*70)
                print()

    def graficar_datos(self):
        if self.verboso:
            print('GRAFICANDO GEODATAFRAMES...')
        lista_colores = {'cantones-activos': 'OrRd', 'cantones-recuperados': 'Greens', 'cantones-positivos': 'Purples', 'cantones-fallecidos': 'Greys'}

        for key in self.lista_df_limpio.keys():
            self.graficar_df(key, 'DATA', lista_colores[key], self.fecha_inicial, self.fecha_final)

        if self.verboso:
            print('GRAFICACION FINALIZADO')
            print()
        
    def pdf(self):
        if self.verboso:
            print('CREANDO PDF...')

        self.crear_pdf()
        
        if self.verboso:
            print('PDF CREADO')
            print()


Data = DFs(mostrar_graficas=True, verboso=True)
Data.main()
