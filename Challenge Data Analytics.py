#-*- coding: utf  -8 -*-

from ast import Index
from codecs import utf_16_decode, utf_32_decode, utf_8_decode, utf_8_encode
from copy import copy
from encodings import utf_8
import encodings
from operator import concat
from xml.sax import default_parser_list
import pandas as pd
from sqlalchemy import create_engine
import requests as rq

################################################ DESCARGA DE LOS ARCHIVOS CSV ####################################################
#ES NECESARIO ACLARAR QUE EN ESTE BLOQUE NO ES POSIBLE REALIZAR LA DESCARGA DEL ARCHIVO .CSV DE CINES, YA QUE DICHA URL PRESENTA
#UN ERROR, EL CUAL NO PERMITE ABRIR DICHA PAGINA Y DESCARGAR EL ARCHIVO.
#YO ME CONTACTE CON ALKEMY MEDIANTE INSTAGRAM PARA INDICAR DICHO INCONVENIENTE Y POR ESE MEDIO ME PASAR EL LINK DE GOOGLEDRIVE PARA 
#PODER DESCARGA LA TABLA QUE PRESENTABA DICHO INCONVENIENTO.
#EL TEMA ES QUE AL DESCARGAR MEDIANTE LA LIBRERIA REQUESTS LOS ARCHIVOS DESDE GOOGLE DRIVE, PRESENTABAN PROBLEMAS DE CODIFICACION
#AL PROBAR LA DESCARGA DESDE LA PAGINA ORIGINAL, LOS ARCHIVOS SE DESCARGAN SIN NINGUN INCONVENIENTE. ES POR ESO QUE EN EL CODIGO
#SE PRESENTA LA DESCARGA SOLO DE MUSEO Y BIBLIOTECA, PERO EL PROCEDIMIENTO ES EL MISMO.

################################################# DESCARGA ARCHIVO .CSV MUSEO ####################################################
req = rq.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv')
url_content = req.content
csv_file = open('C:/Users/Leandro/Documents/PYTHON/Archivos/Museo-2022-septiembre-museo-13-09-2022', 'wb')
csv_file.write(url_content)
csv_file.close()
##################################################################################################################################

############################################### DESCARGA ARCHIVO .CSV BIBLIOTECA #################################################
req = rq.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv')
url_content = req.content
csv_file = open('C:/Users/Leandro/Documents/PYTHON/Archivos/Biblioteca-2022-septiembre-biblioteca-13-09-2022', 'wb')
csv_file.write(url_content)
csv_file.close()
##################################################################################################################################

################################################## DESCARGA ARCHIVO .CSV CINE ####################################################
#COMO SE COMENTO ANTERIORMENTE, NO ES POSIBLE DESCARGAR EL ARCHIVO MEDIANTE REQUESTS DESDE LA PAGINA DATOS.GOB.AR, YA QUE DICHA
#URL PRESENTA UN ERROR.
##################################################################################################################################

##################################################################################################################################


##################################################################################################################################
#EN ESTOS TRES BLOQUES SIGUIENTES, LO QUE REALIZAMOS ES ABRIR LOS 3 ARCHIVOS CSV, LOS QUE CORRESPONDEN A MUSEO, CINE Y BIBLIOTECA
#LUEGO NORMALIZAMOS LOS 3 ARCHIVOS PARA QUE AL CONCATENAR, LOS 3 TENGAN LAS COLUMNANS CON LOS MISMO NOMBRES Y SE REALICE 
#CORRECTAMENTE LA CONCATENACI??N.
#POR ULTIMO SE LE AGREGA A CADA ARCHIVO UNA COLUMNA NUEVA, LA CUAL TIENE EL NOMBRE DE TIPO. EN DICHA COLUMNA SE LE AGREGA
#LA PALABRA MUSEO AL ARCHIVO DE MUSEO, BIBLIOTECA AL ARCHIVO DE BIBLIOTECA Y CINE AL ARCHIVO DE CINE. ESTO SE REALIZA PARA 
#QUE AL REALIZAR EL PROCESAMIENTO DE DATOS EN EL ARCHIVO CONCATENADO (DONDE ESTAN LOS DATOS DE LOS TRES ARCHIVOS), SE PUEDA
#FILTRAR DE MANERA RAPIDA Y CORRECTA PARA BUSCAR INFORMACION DEL ESTABLECIMIENTO QUE DESEAMOS (DE LOS 3 QUE EXISTE, MUSEO,
# BIBLIOTECA Y CINE)
############################################# ABRIMOS ARCHIVO MUSEOS Y NORMALIZAMOS ##############################################
dfMus = pd.read_csv('C:/Users/Leandro/Documents/PYTHON/Archivos/Museo-2022-septiembre-museo-13-09-2022.csv')
dfMus = dfMus.rename(columns={'categoria':'Categoria', 'provincia':'Provincia', 'localidad':'Localidad', 'nombre':'Nombre' ,'direccion':'Domicilio',
 'telefono':'Telefono', 'subcategoria':'Subcategoria', 'piso':'Piso', 'cod_area':'Cod_Area', 'fuente':'Fuente', 'jurisdiccion':'Jurisdiccion',
 'a??o_inauguracion':'A??o_Inauguracion', 'actualizacion':'Actualizacion'})
dfMus['Tipo'] = pd.Series(['MUSEO'])
dfMus['Tipo'] = dfMus[['Tipo']].fillna('MUSEO')
########################################################################################################################################

########################################### ABRIMOS ARCHIVO BIBLIOTECAS Y NORMALIZAMOS ###########################################
dfBib = pd.read_csv('C:/Users/Leandro/Documents/PYTHON/Archivos/Biblioteca-2022-septiembre-biblioteca-13-09-2022.csv')
dfBib = dfBib.rename(columns={'Observacion':'Observaciones', 'Categor??a':'Categoria', 'Tel??fono':'Telefono', 'Cod_tel':'Cod_Area',
 'Informaci??n adicional':'Info_adicional', 'a??o_inicio':'A??o_Inaguracion', 'A??o_actualizacion':'Actualizacion'})
dfBib['Tipo'] = pd.Series(['BIBLIOTECA'])
dfBib['Tipo'] = dfBib[['Tipo']].fillna('BIBLIOTECA')
########################################################################################################################################

######################################################## ABRIMOS ARCHIVO CINES Y NORMALIZAMOS ###############################################################
dfCin = pd.read_csv('C:/Users/Leandro/Documents/PYTHON/Archivos/Cine-2022-septiembre-cine-13-09-2022.csv')
dfCin = dfCin.rename(columns={'Tel??fono':'Telefono','Categor??a':'Categoria', 'Direcci??n':'Domicilio', 'cod_area':'Cod_Area',
 'Informaci??n adicional':'Info_adicional', 'a??o_actualizacion':'Actualizacion', 'tipo_gestion':'Tipo_gestion'})
dfCin['Tipo'] = pd.Series(['CINE'])
dfCin['Tipo'] = dfCin[['Tipo']].fillna('CINE')
###################################################################################################################################################################
#ACA TERMINAN LOS 3 BLOQUES QUE SE EXPLICARON EN LA PARTE SUPERIOR

###################################################################################################################################################################

################################################# CONCATENAMOS ARCHIVOS FILTRADOS, CREANDO UN SOLO DATAFRAME ######################################################
dfConcat = pd.concat([dfMus, dfBib, dfCin]) # concatenamos para luego trabajar simulando que el archivo inicial es este
########################################################################################################################################

################################################# GENERAMOS DATAFRAME CON COLUMNAS DESEADAS ###############################################
dfFinal = dfConcat[['Tipo', 'Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoria', 'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'CP', 'Telefono', 'Mail', 'Web']]
###########################################################################################################################################

######################################### EXTRAEMOS DATOS DEL DATAFRAME CONCATENADO ###########################################
dfCat = dfConcat['Categoria']
dfCat = dfCat.dropna()
RegCat = len(dfCat) # RegCat representa la cantidad de registros por categor??a
dfFuen = dfConcat['Fuente']
dfFuen = dfFuen.dropna()
RegFuen = len(dfFuen) # RegFuen representa la cantidad de registros por fuentes
dfProbCat = dfConcat[['Provincia','Categoria']]
dfProbCat = dfProbCat.dropna()
RegProbCat = len(dfProbCat) # RegProbCat representa la cantidad de registros  por proviancia y categoria


datos = {'REGISTRO':['Por Cat', 'Por Tel', 'Por Pro y Cat'],
'CANTIDAD DE REGISTROS':[RegCat, RegFuen, RegProbCat]}
print("")
datosD = pd.DataFrame(datos)
datosD = datosD.set_index(keys = 'REGISTRO') # datosD es un dataframe con la cantidad de registros de las variables anteriores
# print(datosD)
###############################################################################################################################

########################################## PROCESAMOS TABLA PARA OBTENER DATOS DE CINE ########################################
CineFinal = dfConcat[dfConcat['Tipo']=='CINE']
CineFinal = CineFinal[['Provincia','Pantallas','Butacas','espacio_INCAA']] # Es un dataframe con las columnas solicitadas de cine
# print(CineFinal)
###############################################################################################################################

######################################### CARGAMOS TABLAS REALIZADAS A LA BASE DE DATOS #######################################
engine = create_engine("postgresql://postgres:34SQL110@localhost:5432/Alkemy") # deriba de la libreria sqlalchemy
con = engine.connect() # se conecta a la base de datos que se indica en engine
dfFinal.to_sql("TABLA FINA", con=engine, if_exists="replace")
datosD.to_sql("REGISTROS", con=engine, if_exists="replace")
CineFinal.to_sql("CINE",con=engine, if_exists="replace")
engine.execute("SELECT * from TABLA FINAL", "SELECT * from REGISTROS", "SELECT * from CINE").fetchall()
con.close()
###############################################################################################################################