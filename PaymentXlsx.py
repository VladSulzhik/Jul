import warnings
import pandas as pd
import os
import re
import pathlib
from datetime import datetime

warnings.simplefilter(action='ignore')

start_time = datetime.now()
print(f'Script started at: {start_time}')

#Brandshop & Brandshop продавцы удалить строку store (3)
year = 2022
month = 11

csv_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/Сверка {year}_{month}.csv'

files_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/raw_data'
files = os.listdir(files_path)

mapping_list = {'Bomba': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Brandshop': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0},
                'Brandshopпродавцы': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0},
                'BrandshopHaier': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'ComputersHaier': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'DNSKBTKBTTV': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0},
                'DNSTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Domotehnika': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'DomotehnikaTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Eldorado': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'EldoradoMBTKlimat': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1},
                'EldoradoTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Eldoradovstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Eleks': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Holodilnikru': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'HolodilnikruTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'HolodilnikruVstroykaMBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'KorporaciyaCentr': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'MoyaRodnya': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'MVideo': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'MVideoMBTKlimat': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1},
                'MvideoTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'MVideovstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Orbita': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'OrbitaVstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Patio': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'PatioMBT': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'PatioOnlayn': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'PoiskBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'RBT': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'RBTVstroyka': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'SulpakKBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'SulpakTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'TelepromouteryFM': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'Voltmart': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'YuSTKBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1},
                'СверкапродажTyphoon': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1},
                }

dataframes_list = []

def xlsxParser(file_name, chain, index_columns, number_of_data_columns, adress_row, adress_column):
    raw_df = pd.read_excel(files_path+'/'+file_name+'.xlsx',
                           sheet_name='Сверка продаж', header=None)

    raw_df = raw_df.iloc[:raw_df[0].isna().drop_duplicates(keep='last').index[0]+1]

    base = raw_df[raw_df.columns[:5]]
    base.columns = ['Date', 'Brand', 'Category', 'Model', 'MotivationPercent']
    base = base.iloc[7:]

    others_columns = raw_df[raw_df.columns[index_columns:]]
    others_columns = others_columns.iloc[:, :(len(others_columns.columns) // number_of_data_columns) * number_of_data_columns]

    temp_df = pd.DataFrame()

    for start_index in range(0, len(others_columns.columns), number_of_data_columns):
        temp_pair_of_columns = others_columns.iloc[:, start_index:start_index + 2]
        temp_pair_of_columns.columns = ['Quantity', 'Total']
        temp_pair_of_columns['Agency'] = temp_pair_of_columns.iloc[0, 0]
        temp_pair_of_columns['City'] = temp_pair_of_columns.iloc[1, 0]
        temp_pair_of_columns['Adress'] = str(temp_pair_of_columns.iloc[adress_row, adress_column]).replace('\n', ' ')
        temp_pair_of_columns['Code'] = temp_pair_of_columns.iloc[2, 0]
        temp_pair_of_columns['Chain'] = chain
        temp_pair_of_columns['Source'] = file_name
        temp_pair_of_columns['Month'] = month
        temp_pair_of_columns['Year'] = year

        temp_pair_of_columns = temp_pair_of_columns.iloc[7:]
        temp_pair_of_columns = temp_pair_of_columns.dropna(subset=['Quantity'])
        temp_df = temp_df.append(temp_pair_of_columns)

    result = base.join(temp_df, how='inner')
    dataframes_list.append(result)

for file in files:
    file_name = pathlib.Path(file).stem
    chain = file_name.split('_')[0]
    file_name_clean = re.sub('[^a-zA-Zа-яА-Я]+', '', file_name)
    #print(file_name, file_name_clean)
    if file_name_clean in mapping_list.keys():
        index_columns = mapping_list[file_name_clean]['index_columns']
        number_of_data_columns = mapping_list[file_name_clean]['number_of_data_columns']
        adress_row = mapping_list[file_name_clean]['adress_row']
        adress_column = mapping_list[file_name_clean]['adress_column']
        xlsxParser(file_name, chain, index_columns, number_of_data_columns, adress_row, adress_column)
    else:
        print(file+' not loaded - check column index!')

df = pd.concat(dataframes_list)
df.to_csv(csv_path, encoding='utf-8-sig', sep='\t', index=False)

end_time = datetime.now()
duration = end_time - start_time

print(f'Script ended at: {end_time} \nWorking time: {duration}')


