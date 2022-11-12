# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 14:25:03 2022

GlowByte Autumn Hack 2022
Team 8

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
import os
#  from ftplib import FTP_TLS
import ftplib
import pandas as pd
import glob

def waybill_fill(engine, ftps_login, ftps_pass):
    #очистим временные файлы
    for f in glob.glob(os.path.abspath(os.curdir)+'\\xml\\'+"*.xml"):
        os.remove(f)
        
    # Подключение к FTP  выбор папки waybills и сбор названия файлов
    ftps = ftplib.FTP_TLS(timeout=200)
    ftps.connect('de-edu-db.chronosavant.ru', 21)
    ftps.auth()
    ftps.prot_p()
    ftps.login(ftps_login, ftps_pass)
    ftps.cwd('/waybills/')
    filenames = ftps.nlst()

    #запросим все обработанные строки ДБ dwh_samara.fact_waybills
    query = '''SELECT waybill_num
            FROM dwh_samara.fact_waybills AS wb'''
    last_waybill_pd = pd.read_sql_query(query, con=engine)
    
    #подготовим список имен уже обработанных файлов
    last_waybill_pd = last_waybill_pd.apply(lambda x: x+'.xml')
    wb_old = last_waybill_pd['waybill_num'].tolist()
    
    #получим список необработанных
    diff = list(set(filenames) - set(wb_old))
           
    #для всех файлов в списке на сервере проверяем обрабатывался он ранее или нет
    #если обработки ранее не было - скачиваем файл
    
    path = os.path.abspath(os.curdir)+'\\xml\\'
    for file in filenames:
        if file in diff:
            host_file = os.path.join(path, file)
            try:
                with open(host_file, 'wb') as local_file:
                    ftps.retrbinary('RETR ' + file, local_file.write)
                print(file, 'Done')
            except ftplib.error_perm:
                print(str(ftplib.error_perm))
    
    #Закроем ftps соединение
    ftps.close()
    
    #получаем спискок скачанных файлов
    all_files = glob.glob(os.path.join(path, "*.xml"))
    
    #производим парсинг интересующих нас данных из файлов
    li = []
    for filename in all_files:
        try:
            df1 = pd.read_xml(filename, xpath = ".//waybill")
            df2 = pd.read_xml(filename, xpath = ".//driver")
            df3 = pd.read_xml(filename, xpath = ".//period")
            df = pd.concat([df1,df2,df3], axis=1)
            df['number'] = filename[-18:-4]
            li.append(df)
        except Exception as e:
            print("ERROR : "+str(e)) #при ошибках в загрузке файлов скрипт не сломается
    
    #если получили не пустой список данных - обработаем его
    if len(li) > 0:
        #готовим DataFrame для загрузки данных
        waibills = pd.concat(li, axis=0, ignore_index=True)
        waibills['driver_pers_num'] = waibills['license'].apply(lambda x: ''.join(x.split()))
        waibills = waibills.drop(['driver','period', 'name',
                 'validto','model','license'],
                 axis=1)
        waibills = waibills.rename(columns= {'number': 'waybill_num', 'issuedt': 'issue_dt', 
                                       'car': 'car_plate_num', 'start': 'work_start_dt',
                                       'stop': 'work_end_dt'
                                      })
        
        #сформируем строку для отправки данных в запросе INSERT INTO 
        str_to_append = ''
        iterator = zip(waibills['waybill_num'],
                       waibills['issue_dt'],
                       waibills['car_plate_num'], 
                       waibills['work_start_dt'], 
                       waibills['work_end_dt'],
                       waibills['driver_pers_num'])
        for waybill_num, issue_dt, car_plate_num, work_start_dt, work_end_dt, driver_pers_num in iterator:
            str_to_append  = str_to_append + f"('{waybill_num}', '{issue_dt}', '{car_plate_num}', '{work_start_dt}', '{work_end_dt}', '{driver_pers_num}'),"
    
        #если данные str_to_append не пустые - завершим запрос и отправим данные в БД
        if (len(str_to_append)>0):
            query ='INSERT INTO dwh_samara.fact_waybills (waybill_num, issue_dt, car_plate_num, work_start_dt, work_end_dt, driver_pers_num) VALUES '
            query = query + str_to_append
            query = query[:-1]
            engine.execute(query)

    #очистим временные файлы
    for f in glob.glob(os.path.abspath(os.curdir)+'\\xml\\'+"*.xml"):
        os.remove(f)
    
    return 'Fact_waybill filling is finished'

if __name__ == "__main__":
    pass