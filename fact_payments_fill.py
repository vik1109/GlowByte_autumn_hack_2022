# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 21:04:59 2022

GlowByte Autumn Hack 2022
Team 8

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
import ftplib
import pandas as pd
import os
import glob

def fact_paymets_fill_func(engine_samara_db, ftps_login, ftps_pass):
    '''
    Parameters
    ----------
    engine_samara_db : sqlalchemy.engine.base.Engine
        текущее подключение к БД DHW_SAMARA.

    Returns
    -------
    str
        информационная строка, сигнал успешной работы функции.

    '''

    #установка рабочей директории
    path = os.path.abspath(os.curdir)+'\\csv\\'
    
    #предварительная очистка рабочей лиректории, удаление файлов csv
    for f in glob.glob(path + "*.csv"):
        os.remove(f)
    
    # Подключение к FTP  выбор папки payments и сбор названия файлов
    ftps = ftplib.FTP_TLS(timeout=200)
    ftps.connect('de-edu-db.chronosavant.ru', 21)
    ftps.auth()
    ftps.prot_p()
    ftps.login(ftps_login, ftps_pass)
    ftps.cwd('/payments/')
    filenames = ftps.nlst()
    
    #получение данных о датаз ранее загруженных файлов
    query = """
    SELECT DISTINCT (date_trunc('hour', transaction_dt) + ceil(date_part('minute', transaction_dt)::decimal / 30) * interval '30 min') AS transaction_dt
    FROM fact_payments
    ORDER BY transaction_dt
    """
    #запрос 
    pay_dt = pd.read_sql_query(query, con=engine_samara_db)
    
    def file_name(x):
        '''
        Parameters
        ----------
        x : datetime64[ns]
            дата-время транзакции.

        Returns
        -------
        str
            название файла, который мы уже обработали.

        '''
        year = str(x.year)
        if x.day < 10:
            day = 0 + str(x.day)
        else:
            day = str(x.day)
        if x.month < 10:
            month = '0'+str(x.month)
        else:
            month = str(x.month)
        if x.hour < 10:
            hour = '0' + str(x.hour)
        else:
            hour = str(x.hour)
        if x.minute < 10:
            minute = '0' + str(x.minute)
        else:
            minute = str(x.minute)
            
        return (f'payment_{year}-{month}-{day}_{hour}-{minute}.csv')
    
    #подготовка списка обработанных файлов
    pay_dt['file'] = pay_dt['transaction_dt'].apply(lambda x: file_name(x))
    old_file_name_list = pay_dt['file'].tolist()
    new_file_name_list = list(set(filenames)-set(old_file_name_list)) # new_file_name_list - список уже обработанных файлов
    
    #загрузка новых файлов
    for filename in new_file_name_list:
        host_file = os.path.join(path, filename)
        try:
            with open(host_file, 'wb') as local_file:
                ftps.retrbinary('RETR ' + filename, local_file.write)
                print(filename, 'done')
        except Exception as e:
            print("ERROR : "+str(e))
        
    #Закроем ftps соединение
    ftps.close()
    
    #получим список удачно загруженных файлов
    all_files = glob.glob(os.path.join(path, "*.csv"))
    
    #обработка данных из файлов
    colnames = ['transaction_dt', 'card_num', 'transaction_amt']
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, sep = '\t', names=colnames, header=0)
        li.append(df)
    
    if len(li)>0:
        #подготовка pd.DataFrame 
        try:
            payments = pd.concat(li, axis=0)
            payments['transaction_dt'] = pd.to_datetime(payments['transaction_dt'], format = '%d.%m.%Y %H:%M:%S')
            
            #сформируем строку для отправки данных в запросе INSERT INTO 
            str_to_append = ''
            iterator = zip(payments['transaction_dt'],
                           payments['card_num'],
                           payments['transaction_amt'])
            for transaction_dt, card_num, transaction_amt in iterator:
                str_to_append  = str_to_append + f"('{transaction_dt}', '{card_num}', '{transaction_amt}'),"
            
            #если данные str_to_append не пустые - завершим запрос и отправим данные в БД
            if (len(str_to_append)>0):
                query ='INSERT INTO dwh_samara.fact_payments (transaction_dt, card_num, transaction_amt) VALUES '
                query = query + str_to_append
                query = query[:-1]
                engine_samara_db.execute(query)
                
        except Exception as e:
            print("ERROR : "+str(e))
            
    #очистка папки после обработки
    for f in glob.glob(path + "*.csv"):
            os.remove(f)
            
    return 'Fact_payments filling done'

if __name__ == "__main__":
    pass

