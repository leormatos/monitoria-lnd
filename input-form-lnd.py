import time  # to simulate a real time data, time loop

import numpy as np  # np mean, np random
import pandas as pd 
import streamlit as st 
from streamlit_autorefresh import st_autorefresh
import hashlib

# Application Related Module
import pubchempy as pcp
from pysmiles import read_smiles

from gspread_pandas import Spread,Client
from google.oauth2 import service_account
from gsheetsdb import connect

import pickle
import time
from matplotlib import pyplot as plt
from  matplotlib.ticker import FuncFormatter
from datetime import datetime
import plotly.express as px
import seaborn as sns

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive',
         "https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=scope
)

conn = connect(credentials=credentials)
client = Client(scope=scope,creds=credentials)
data_base_name = "lnd_monitores_database"
data_base = Spread(data_base_name,client = client)

sh = client.open(data_base_name)
worksheet_list = sh.worksheets()

# ------------- PARAMETERS, VARIABLES & SETUPS ------------- 

## PAGE SETUP
st.set_page_config(layout="wide")
st_autorefresh(interval=1 * 60 * 1000, key="dataframerefresh")

## SIDEBAR FILTERS

input_filter = ['Atendimento médico','Controle alimentos','Emissões de carbono','Gestão de recursos','Pesagem de resíduos','Pesquisa de DE&I']
input_producer = 'None'
input_event = ''

## SPREADSHEET MAPPING
spreadsheet_residue = 'residuos'
spreadsheet_dei = 'de&i'
spreadsheet_carbon = 'pegada_carbono'
spreadsheet_resources = 'gestao_recursos'
spreadsheet_food = 'gestao_alimentos'
spreadsheet_hs = 'h&s'
spreadsheet_supplier = 'cadastro_fornecedor'
spreadsheet_operation = 'cadastro_operacao'
spreadsheet_events = 'cadastro_evento'
spreadsheet_producer = 'cadastro_produtora'

## DATAFRAME STRUCTURE
columns_carbon = ['timestamp','no_producer','no_event','tp_operation','no_supplier','ds_vehicle_model','tp_fuel','nu_km']
columns_dei = ['timestamp','no_producer','no_event','tp_operation','id_hash_person','nu_age','ds_gender','st_trans','ds_race','st_pcd']
columns_residue = ['timestamp','no_producer','no_event','tp_operation','dt_ref','tp_residue','nu_weight','vl_profit']
columns_resources = ['timestamp','no_producer','no_event','tp_operation','dt_ref','tp_resource','nu_measurement']
columns_food = ['timestamp','no_producer','no_event','tp_operation','no_supplier','st_technical_info','st_small_portion','st_healthy_offering','st_reduced_sugar_salt','st_organic','st_whole_use','st_food_wasting','st_training','st_food_wasting_goals','st_recycling_plan']
columns_hs = ['timestamp','no_producer','no_event','tp_operation','no_supplier','dt_ref','id_hash_person','tp_child_adult','ds_gender','ds_case','tp_health_procedure','st_removal']

## SELECTBOX OPTIONS
tp_residue = ['Alumínio','Orgânico','Papel/Papelão','Plástico','Rejeitos','Vidro']
tp_gender = ['Homem','Mulher','Não binário','Prefiro não declarar']
tp_race = ['Branco','Pardo','Preto', 'Prefiro não declarar']
tp_binary_dei = ['Sim', 'Não', 'Prefiro não declarar']
tp_fuel = ['Diesel','Etanol','Elétrico','Gasolina','Gasolina aeronáutica']
tp_resource = ['Água','Energia','Gás']
tp_binary_food = ['Sim','Não','Não se aplica']
tp_child_adult = ['Adulto','Criança']
tp_health_procedure = ['Curativo','Limpeza','Medicação','Orientação','Sutura','Suporte a crise']
tp_binary_hs = ['Sim','Não','Não se aplica']

# ------------- FUNCTIONS ------------- 

def send_form (opt,spreadsheet_choice,columns):
    df_opt = pd.DataFrame(opt)
    worksheet = sh.worksheet(spreadsheet_choice)
    
    df = pd.DataFrame(worksheet.get_all_records())
    result_df = pd.concat([df, df_opt], ignore_index=False)

    data_base.df_to_sheet(result_df[columns],sheet = spreadsheet_choice,index = False)
    st.info('Registro enviado')

# ------------------- CREATE DATAFRAMES & SIDEBARFILTERS ---------------------

df_operation = pd.DataFrame(sh.worksheet(spreadsheet_operation).get_all_records())
tp_operation = df_operation['tp_operation'].sort_values(ascending=True)

df_producers = pd.DataFrame(sh.worksheet(spreadsheet_producer).get_all_records())
no_producers = df_producers['no_producer'].sort_values(ascending=True)

input_producer = st.sidebar.selectbox('Produtora',no_producers)

df_events = pd.DataFrame(sh.worksheet(spreadsheet_events).get_all_records())
df_events = df_events[(df_events['no_producer']==input_producer)]
no_events = df_events['no_event'].sort_values(ascending=True)

input_event = st.sidebar.selectbox('Evento',no_events)

df_supplier = pd.DataFrame(sh.worksheet(spreadsheet_supplier).get_all_records())
df_supplier = df_supplier[(df_supplier['no_event']==input_event) & (df_supplier['no_producer']==input_producer)]
no_supplier = df_supplier['no_supplier'].sort_values(ascending=True)

input_choice = st.sidebar.selectbox('Formulário',input_filter)


# ------------------- BODY ---------------------
spacer1, header, spacer2 = st.columns((.1, 4, .1))
with header:
    st.title(f"{input_choice}")

if input_choice == 'Atendimento médico':

    spreadsheet_choice = spreadsheet_hs
    columns = columns_hs

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='scale_input_form'):
            
            timestamp = datetime.now()
            input_tp_operation = 'Posto médico'
            input_no_supplier = 'Posto médico'
            input_dt_ref = st.date_input('Data de atendimento')
            input_tp_child_adult = st.selectbox('A pessoa era criança ou adulto?',tp_child_adult)
            input_ds_gender = st.selectbox('Como a pessoa se declara quanto a gênero?',tp_gender)
            input_ds_case = st.text_area('Descrição da ocorrência:')
            input_tp_health_procedure = st.multiselect('Procedimentos realizados(selecione todos):',tp_health_procedure)
            input_st_removal = st.selectbox('A pessoa foi removida do evento?',tp_binary_hs)
            confirm_send = st.form_submit_button(label='Registrar')
            if confirm_send:
                hash_input = input_dt_ref.strftime("%d-%m-%Y")+input_tp_operation+input_no_supplier+input_tp_child_adult+input_ds_gender+input_ds_case
                input_id_hash_person = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
                opt = {
                    'timestamp':[timestamp],
                    #Melhoria 1
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    'tp_operation':[input_tp_operation],
                    'no_supplier':[input_no_supplier],
                    'dt_ref':[input_dt_ref],
                    'id_hash_person': [input_id_hash_person],
                    'tp_child_adult':[input_tp_child_adult],
                    'ds_gender':[input_ds_gender],
                    'ds_case':[input_ds_case],
                    'tp_health_procedure':[input_tp_health_procedure],
                    'st_removal':[input_st_removal]
                }
                send_form(opt,spreadsheet_choice,columns)
elif input_choice == 'Controle alimentos':

    spreadsheet_choice = spreadsheet_food
    columns = columns_food

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='scale_input_form'):
            
            timestamp = datetime.now()

            input_tp_operation = tp_operation
            input_no_supplier = st.selectbox('Empresa',no_supplier)
            input_tp_technical_info = st.selectbox('A empresa possui e utiliza as fichas técnicas de preparação ( instrumento para gerenciar, planejar, orientar o modo de preparo, incluir custos, desperdícios, per capita, rendimento e cálculo nutricional) para realizar as preparações?',tp_binary_food)
            input_tp_small_portion = st.selectbox('A empresa tem opções de porções menores separadamente ou um cardápio infantil?',tp_binary_food)
            input_tp_healthy_offering = st.selectbox('A empresa  oferece  ≥   50%   das suas preparações mais saudáveis?',tp_binary_food)
            input_tp_educed_sugar_salt = st.selectbox('A empresa tem compromissos para reduzir o uso do açúcar, sal ou gordura saturada no cardápio?',tp_binary_food)
            input_tp_organic = st.selectbox('Pelo menos 50% das frutas e vegetais que a empresa compra possuem certificado orgânico?',tp_binary_food)
            input_tp_whole_use = st.selectbox('A empresa prioriza o aproveitamento integral dos alimentos, produzindo preparações seguras que utilizam cascas, talos e/ou aparas comestíveis de vegetais e frutas como ingredientes?',tp_binary_food)
            input_tp_food_wasting = st.selectbox('A empresa avalia seu desperdício de alimentos durante o preparo e a distribuição da comida?',tp_binary_food)
            input_tp_training = st.selectbox('A empresa treina seus funcionários para evitar o desperdício de alimentos durante todas as etapas de produção das refeições, desde o recebimento dos gêneros até a distribuição?',tp_binary_food)
            input_tp_food_wasting_goals = st.selectbox('A empresa apresenta metas para a redução/controle do desperdício de alimentos?',tp_binary_food)
            input_tp_recycling_plan = st.selectbox('A empresa possui um plano de reciclagem dos resíduos gerados?',tp_binary_food)
            confirm_send = st.form_submit_button(label='Registrar')
            
            if confirm_send:
                opt = {
                    'timestamp':[timestamp],
                    #Melhoria 1
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    'tp_operation':[input_tp_operation],
                    'no_supplier':[input_no_supplier],
                    'st_technical_info':[input_tp_technical_info],
                    'st_small_portion':[input_tp_small_portion],
                    'st_healthy_offering':[input_tp_healthy_offering],
                    'st_reduced_sugar_salt':[input_tp_educed_sugar_salt],
                    'st_organic':[input_tp_organic],
                    'st_whole_use':[input_tp_whole_use],
                    'st_food_wasting':[input_tp_food_wasting],
                    'st_training':[input_tp_training],
                    'st_food_wasting_goals':[input_tp_food_wasting_goals],
                    'st_recycling_plan':[input_tp_recycling_plan]

                }
                send_form(opt,spreadsheet_choice,columns)

elif input_choice == 'Emissões de carbono':

    spreadsheet_choice = spreadsheet_carbon
    columns = columns_carbon

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='scale_input_form'):
            
            timestamp = datetime.now()
            input_tp_operation = st.selectbox('Operação',tp_operation)
            input_no_supplier = st.selectbox('Nome do fornecedor',no_supplier)
            input_ds_vehicle_model = st.text_input('Modelo do veículo')
            input_tp_fuel = st.selectbox('Tipo de combustível',tp_fuel)
            input_nu_km = st.number_input('Km rodados',step=1)
            confirm_send = st.form_submit_button(label='Registrar')
            
            if confirm_send:
                opt = {
                    'timestamp':[timestamp],
                    #Melhoria 1
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    'tp_operation':[input_tp_operation],
                    'no_supplier':[input_no_supplier],
                    'ds_vehicle_model':[input_ds_vehicle_model],
                    'tp_fuel':[input_tp_fuel],
                    'nu_km':[input_nu_km]
                }
                send_form(opt,spreadsheet_choice,columns)

elif input_choice == 'Gestão de recursos':

    spreadsheet_choice = spreadsheet_resources
    columns = columns_resources

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='scale_input_form'):
            
            timestamp = datetime.now()
            input_tp_operation = st.selectbox('Operação',tp_operation)
            input_dt_ref = st.date_input('Data de referência')
            input_tp_resource = st.selectbox('Tipo de recurso',tp_resource)
            input_nu_measurement = st.number_input('Medição',step=0.5)
            confirm_send = st.form_submit_button(label='Registrar')
            
            if confirm_send:
                opt = {
                    'timestamp':[timestamp],
                    #Melhoria 1
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    'tp_operation':[input_tp_operation],
                    'dt_ref':[input_dt_ref],
                    'tp_resource':[input_tp_resource],
                    'nu_measurement':[input_nu_measurement]
                }
                send_form(opt,spreadsheet_choice,columns)

elif input_choice == 'Pesagem de resíduos':

    spreadsheet_choice = spreadsheet_residue
    columns = columns_residue

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='scale_input_form'):
            
            timestamp = datetime.now()
            #Melhoria 1
            input_tp_operation = st.selectbox('Operação',tp_operation)
            input_dt_ref = st.date_input('Data de referência')
            input_tp_residue = st.selectbox('Tipo de resíduo',tp_residue)
            input_nu_weight = st.number_input('Peso medido (kg)',step=0.5)
            input_vl_profit = st.number_input('Renda gerada (R$)', step=0.05)
            confirm_send = st.form_submit_button(label='Registrar')
            
            if confirm_send:
                opt = {
                    'timestamp':[timestamp],
                    #Melhoria 1
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    'tp_operation':[input_tp_operation],
                    'dt_ref':[input_dt_ref],
                    'tp_residue':[input_tp_residue],
                    'nu_weight':[input_nu_weight],
                    'vl_profit':[input_vl_profit]
                }
                send_form(opt,spreadsheet_choice,columns)

elif input_choice == 'Pesquisa de DE&I':

    spreadsheet_choice = spreadsheet_dei
    columns = columns_dei

    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        with st.form(key='dei_input_form'):
            
            timestamp = datetime.now()
            #Melhoria 1
            input_tp_operation = st.selectbox('Operação',tp_operation)
            input_nu_age = st.slider('Qual sua idade?',18,90,1)
            input_ds_gender = st.selectbox('Declaração de gênero',tp_gender)
            input_st_trans = st.selectbox('Você se declara Trans?',tp_binary_dei)
            input_ds_race = st.selectbox('Como você se declara em relação a cor?',tp_race)
            input_st_pcd = st.selectbox('PCD - Possui alguma deficiência?',tp_binary_dei)
            confirm_send = st.form_submit_button(label='Registrar')
            
            if confirm_send:

                hash_input = timestamp.strftime("%d-%m-%Y")+input_tp_operation+str(input_nu_age)+input_ds_gender+input_st_trans+input_ds_race+input_st_pcd    
                input_id_hash_person = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
                opt = {
                    'timestamp':[timestamp],
                    'no_producer':[input_producer],
                    'no_event':[input_event],
                    #Melhoria 1
                    'tp_operation':[input_tp_operation],
                    'id_hash_person':[input_id_hash_person],
                    'nu_age':[input_nu_age],
                    'ds_gender':[input_ds_gender],
                    'st_trans':[input_st_trans],
                    'ds_race':[input_ds_race],
                    'st_pcd':[input_st_pcd]
                }
                send_form(opt,spreadsheet_choice,columns)



