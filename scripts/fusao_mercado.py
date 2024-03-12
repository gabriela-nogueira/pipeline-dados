import json
import csv
from data_processing import Data

path_json = 'raw_data/dados_empresaA.json'
path_csv = 'raw_data/dados_empresaB.csv'

dados_empresaA = Data.load_data(path_json, 'json')
dados_empresaB = Data.load_data(path_csv, 'csv')


print(f"Nome das colunas empresa A: {dados_empresaA.columns}")

print(f"Nome das colunas empresa B: {dados_empresaB.columns}")

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)

print(f"Nome das colunas empresa B após rename: {dados_empresaB.columns}")

dados_combinados = Data.join_data([dados_empresaA, dados_empresaB])

tamanho_dados_combinados = dados_combinados.data_size

print(f"Tamanho do df dos dados combinados: {tamanho_dados_combinados}")

path_dados_combinados = 'data_processed/dados_combinados.csv'

dados_combinados.save_data_to_csv(path_dados_combinados)

print("Processo finalizado com sucesso. ^-^")

