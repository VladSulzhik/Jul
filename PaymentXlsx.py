import warnings
import pandas as pd
import numpy as np
import os
import re
import pathlib
from datetime import datetime

warnings.simplefilter(action='ignore')

start_time = datetime.now()
print(f'Script started at: {start_time}')

#Brandshop & Brandshop продавцы удалить строку store (3) - если 8 строк в шапке
#Удалить лишние столбцы
#Мотивация может быть не в 5 первых ячейках

year = 2022
month = 12

csv_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/Сверка {year}_{month}.csv'
#csv_path = f'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time/Сверки/{year}_{month}/Сверка {year}_{month}.csv' #Vlad
files_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/raw_data'
#files_path = f'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time/Сверки/{year}_{month}/raw_data' #Vlad
files = os.listdir(files_path)

mapping_list = {'Bomba': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Brandshop': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0, 'fix_column': 8},
                'Brandshopпродавцы': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0, 'fix_column': 8},
                'BrandshopHaier': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'ComputersHaier': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'DNSKBTKBTTV': {'index_columns': 10, 'number_of_data_columns': 5, 'adress_row': 3, 'adress_column': 0, 'fix_column': 8},
                'DNSTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Domotehnika': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'DomotehnikaTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Eldorado': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'EldoradoMBTKlimat': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1, 'fix_column': 6},
                'EldoradoTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Eldoradovstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'Eleks': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Holodilnikru': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'HolodilnikruTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'HolodilnikruVstroykaMBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'KorporaciyaCentr': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'MoyaRodnya': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'MVideo': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'MVideoMBTKlimat': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1, 'fix_column': 6},
                'MvideoTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'MVideovstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'Orbita': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'OrbitaVstroyka': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Patio': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'PatioMBT': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'PatioOnlayn': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'PoiskBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'RBT': {'index_columns': 12, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 10},
                'RBTVstroyka': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'SulpakKBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'SulpakTV': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'TelepromouteryFM': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'Voltmart': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'YuSTKBT': {'index_columns': 9, 'number_of_data_columns': 2, 'adress_row': 1, 'adress_column': 1, 'fix_column': 7},
                'СверкапродажTyphoon': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1, 'fix_column': 6},
                }

dataframes_list = []

def xlsxParser(file_name, chain, index_columns, number_of_data_columns, adress_row, adress_column, fix_column):
    raw_df = pd.read_excel(files_path+'/'+file_name+'.xlsx',
                           sheet_name='Сверка продаж', header=None)

    fix_rows = list(np.where(raw_df[fix_column] == 'Фикс. бонус Основа')[0])
    fix2_rows = list(np.where(raw_df[fix_column] == 'Фикса 2')[0])

    print(fix_rows, fix2_rows)

    total_sum = raw_df.iloc[6, 6]
    raw_df = raw_df.iloc[:raw_df[0].isna().drop_duplicates(keep='last').index[0]+1]

    base = raw_df[raw_df.columns[:5]]
    base.columns = ['Date', 'Brand', 'Category', 'Model', 'MotivationPercent']
    base = base.iloc[7:]

    others_columns = raw_df[raw_df.columns[index_columns:]]
    others_columns = others_columns.iloc[:, :(len(others_columns.columns) // number_of_data_columns) * number_of_data_columns]
   
    #fix_df = fix_df.iloc[fix_rows, index_columns :(len(others_columns.columns) // number_of_data_columns) * number_of_data_columns]

    temp_df = pd.DataFrame()

    for start_index in range(0, len(others_columns.columns), number_of_data_columns):
        temp_pair_of_columns = others_columns.iloc[:, start_index:start_index + 2]
        temp_pair_of_columns.columns = ['Quantity', 'Total']
        temp_pair_of_columns['CheckTotal'] = total_sum
        temp_pair_of_columns['Agency'] = temp_pair_of_columns.iloc[0, 0]
        temp_pair_of_columns['City'] = temp_pair_of_columns.iloc[1, 0]
        temp_pair_of_columns['Adress'] = str(temp_pair_of_columns.iloc[adress_row, adress_column]).replace('\n', ' ')
        temp_pair_of_columns['Code'] = temp_pair_of_columns.iloc[2, 0]
        temp_pair_of_columns['Chain'] = chain
        temp_pair_of_columns['Source'] = file_name
        temp_pair_of_columns['Month'] = month
        temp_pair_of_columns['Year'] = year
        temp_pair_of_columns['FixBase'] = temp_pair_of_fix_columns.iloc[0, 0]
        temp_pair_of_columns['Fix2'] = temp_pair_of_fix_columns.iloc[1, 0]

        temp_pair_of_columns = temp_pair_of_columns.iloc[7:]
        temp_pair_of_columns = temp_pair_of_columns.dropna(subset=['Total'])
        temp_df = temp_df.append(temp_pair_of_columns)

    result = base.join(temp_df, how='inner')
    dataframes_list.append(result)

for file in files:
    file_name = pathlib.Path(file).stem
    chain = file_name.split('_')[0]
    file_name_clean = re.sub('[^a-zA-Zа-яА-Я]+', '', file_name)

    if file_name_clean in mapping_list.keys():
        index_columns = mapping_list[file_name_clean]['index_columns']
        number_of_data_columns = mapping_list[file_name_clean]['number_of_data_columns']
        adress_row = mapping_list[file_name_clean]['adress_row']
        adress_column = mapping_list[file_name_clean]['adress_column']
        fix_column = mapping_list[file_name_clean]['fix_column']
        xlsxParser(file_name, chain, index_columns, number_of_data_columns, adress_row, adress_column, fix_column)
    else:
        print(file+' not loaded - new file!')

df = pd.concat(dataframes_list)
df.to_csv(csv_path, encoding='utf-8-sig', sep='\t', index=False)

end_time = datetime.now()
duration = end_time - start_time

print(f'Script ended at: {end_time} \nWorking time: {duration}')


