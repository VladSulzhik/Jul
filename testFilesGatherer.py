import os
import pandas as pd
import numpy as np
import datetime

path = r'C:/Users/vlads/OneDrive/Рабочий стол/DataView/0_Projects/Time'
files = os.listdir(path)
files_xls = [f for f in files if f[-3:] == 'xls' or f[-4:] == 'xlsx']

dataframes_list = []
#new comment
for file in files_xls:
    file_name = file.replace('_', ' ')
    file_name = file_name[:file_name.rfind('.')]
    chain = file_name.split(' ')[1]
    date = file_name.split(' ')[-1]
    week_number = datetime.date(
                                int(date.split('.')[2]),
                                int(date.split('.')[1]),
                                int(date.split('.')[0])
                                ).isocalendar().week-1
    week_number = "W"+str(week_number)+"_22"
    df = pd.read_excel(path+'/'+file, header=None)
    if chain == 'DNS':
        df = df.iloc[1: , [2,4,7,9]]
        df = df.rename(columns={2:'№ магазина', 4:'Адрес', 7:'Супервайзер ТВ', 9:'Статус магазина'})
    else:
        df = df.iloc[1: , [2,4,5,8]]
        df = df.rename(columns={2:'№ магазина', 4:'Адрес', 5:'Супервайзер ТВ', 8:'Статус магазина'})
    df['Статус Работает'] = np.where(df['Статус магазина']=='Работает',1,0)
    df['Статус Поиск'] = np.where(df['Статус магазина']=='Поиск',1,0)
    df = df.drop_duplicates()
    df = df.groupby(['Супервайзер ТВ']).sum()
    df['Сеть'] = chain
    df['Неделя'] = week_number
    dataframes_list.append(df)

df = pd.concat(dataframes_list)
df.to_excel(path+'/'+'spread.xlsx')
print('Done')