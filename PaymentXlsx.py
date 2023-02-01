import warnings
import pandas as pd
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

#JuliaFiles
sverka_csv_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/Сверка {year}_{month}.csv'
fix_csv_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/Фикс {year}_{month}.csv'
files_path = f'C:/Users/381/OneDrive/Рабочий стол/Time/Сергей С/{year}_{month}/raw_data'

#VladFiles
#sverka_csv_path = f'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time/Сверки/{year}_{month}/Сверка {year}_{month}.csv'
#fix_csv_path = f'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time/Сверки/{year}_{month}/Фикс {year}_{month}.csv'
#files_path = f'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time/Сверки/{year}_{month}/raw_data'

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
                'СверкапродажTyphoon': {'index_columns': 8, 'number_of_data_columns': 3, 'adress_row': 1, 'adress_column': 1, 'fix_column': 6}
                }

dataframes_sverka_list = []
dataframes_fix_list = []

def xlsxParser(file_name, chain, index_columns, number_of_data_columns, adress_row, adress_column, fix_column):
    raw_df = pd.read_excel(files_path+'/'+file_name+'.xlsx',
                           sheet_name='Сверка продаж', header=None)
    total_sum = raw_df.iloc[6, 6]

    raw_df[fix_column].replace('Фикса 2', 'Фикс 2', inplace=True)
    fix_df = raw_df.loc[raw_df[fix_column].isin(['Фикс. бонус Основа', 'Фикс 2', 'Фикс. бонус RF',
                                                 'Фикс. бонус WM', 'Фикс. бонус Встройка', 'Фикс. бонус ТВ'])]
    base_fix = fix_df.loc[:, [fix_column]]
    base_fix.columns = ['FixType']
    other_columns_fix = fix_df.iloc[:,
             index_columns:
             ((len(fix_df.columns)-index_columns) // number_of_data_columns) * number_of_data_columns + index_columns]

    sverka_df = raw_df.iloc[:raw_df[0].isna().drop_duplicates(keep='last').index[0]+1]

    base_sverka = sverka_df[sverka_df.columns[:5]]
    base_sverka.columns = ['Date', 'Brand', 'Category', 'Model', 'MotivationPercent']
    base_sverka = base_sverka.iloc[7:]
    other_columns_sverka = sverka_df.iloc[:,
             index_columns:
             ((len(sverka_df.columns)-index_columns) // number_of_data_columns) * number_of_data_columns + index_columns]

    temp_df_sverka = pd.DataFrame()
    temp_df_fix = pd.DataFrame()

    for start_index in range(0, len(other_columns_sverka.columns), number_of_data_columns):
        temp_columns_sverka = other_columns_sverka.iloc[:, start_index:start_index + 2]
        temp_columns_sverka.columns = ['Quantity', 'Total']
        temp_columns_sverka['CheckTotal'] = total_sum
        temp_columns_sverka['Agency'] = temp_columns_sverka.iloc[0, 0]
        temp_columns_sverka['City'] = temp_columns_sverka.iloc[1, 0]
        temp_columns_sverka['Adress'] = str(temp_columns_sverka.iloc[adress_row, adress_column]).replace('\n', ' ')
        temp_columns_sverka['Code'] = temp_columns_sverka.iloc[2, 0]
        temp_columns_sverka['Chain'] = chain
        temp_columns_sverka['Source'] = file_name
        temp_columns_sverka['SourceClean'] = file_name_clean
        temp_columns_sverka['Month'] = month
        temp_columns_sverka['Year'] = year

        temp_columns_fix = other_columns_fix.iloc[:, start_index:start_index + 2]
        temp_columns_fix.columns = ['Total', 'Additional']
        temp_columns_fix['Agency'] = temp_columns_sverka.iloc[0, 0]
        temp_columns_fix['City'] = temp_columns_sverka.iloc[1, 0]
        temp_columns_fix['Adress'] = str(temp_columns_sverka.iloc[adress_row, adress_column]).replace('\n', ' ')
        temp_columns_fix['Code'] = temp_columns_sverka.iloc[2, 0]
        temp_columns_fix['Chain'] = chain
        temp_columns_fix['Source'] = file_name
        temp_columns_fix['SourceClean'] = file_name_clean
        temp_columns_fix['Month'] = month
        temp_columns_fix['Year'] = year

        temp_columns_sverka = temp_columns_sverka.iloc[7:]
        temp_columns_sverka = temp_columns_sverka.dropna(subset=['Total'])

        temp_df_sverka = temp_df_sverka.append(temp_columns_sverka)
        temp_df_fix = temp_df_fix.append(temp_columns_fix)

    sverka_result = base_sverka.join(temp_df_sverka, how='inner')
    dataframes_sverka_list.append(sverka_result)

    fix_result = base_fix.join(temp_df_fix, how='inner')
    dataframes_fix_list.append(fix_result)

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

sverka_for_csv = pd.concat(dataframes_sverka_list)
sverka_for_csv.to_csv(sverka_csv_path, encoding='utf-8-sig', sep='\t', index=False)

fix_for_csv = pd.concat(dataframes_fix_list)
fix_for_csv.to_csv(fix_csv_path, encoding='utf-8-sig', sep='\t', index=False)

end_time = datetime.now()
duration = end_time - start_time

print(f'Script ended at: {end_time} \nWorking time: {duration}')


