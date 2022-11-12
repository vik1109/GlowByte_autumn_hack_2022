# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 16:04:31 2022

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""

#импорт библиотек
from sqlalchemy import create_engine 
from fact_waybill_fii_with_sql import waybill_fill
from fact_rides import fact_rides_fill
from fact_payments_fill import fact_paymets_fill_func
from rep_drivers_violations import rep_drivers_violations_fill
from rep_drivers_payments import rep_drivers_payments_fill
from rep_drivers_overtime import rep_drivers_overtime_fill
from dim_fillment import dim_cars, dim_clients, dim_drivers
from rep_clients_hist import rep_clients_hist_fill
import os, getpass


if __name__ == "__main__":
    dwh_samara_login = os.getenv('DWH_SAMARA_LOGIN')
    dwh_samara_pass = os.getenv('DWH_SAMARA_PASS')
    
    taxi_login = os.getenv('TAXI_LOGIN')
    taxi_pass = os.getenv('TAXI_PASS')
    
    ftps_login = os.getenv('FTPS_LOGIN')
    ftps_pass = os.getenv('FTPS_PASS')
    
    if not dwh_samara_login:
        dwh_samara_login = getpass.getpass('DWH_SAMARA_LOGIN> ')
    if not dwh_samara_pass:
        dwh_samara_pass = getpass.getpass('DWH_SAMARA_PASS> ')
        
    if not taxi_login:
        taxi_login = getpass.getpass('TAXI_LOGIN> ')
    if not taxi_pass:
        taxi_pass = getpass.getpass('TAXI_PASS> ')
        
    if not ftps_login:
        ftps_login = getpass.getpass('FTPS_LOGIN> ')
    if not ftps_pass:
        ftps_pass = getpass.getpass('FTPS_PASS> ')

    #словарь настроек подключения к базе данных
    db_config = {
        'user': taxi_login, # имя пользователя
        'pwd': taxi_pass, # пароль
        'host': 'de-edu-db.chronosavant.ru',
        'port': 5432, # порт подключения
        'db': 'taxi' # название базы данных
    }  

    #сторока подключения к базе данных
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
        db_config['user'],
        db_config['pwd'],
        db_config['host'],
        db_config['port'],
        db_config['db'],
    )

    #словарь настроек подключения к базе данных
    db_config_dwh_samara = {
        'user': dwh_samara_login, # имя пользователя
        'pwd': dwh_samara_pass, # пароль
        'host': 'de-edu-db.chronosavant.ru',
        'port': 5432, # порт подключения
        'db': 'dwh' # название базы данных
    }  

    #сторока подключения к базе данных
    connection_string_samara = 'postgresql://{}:{}@{}:{}/{}'.format(
        db_config_dwh_samara['user'],
        db_config_dwh_samara['pwd'],
        db_config_dwh_samara['host'],
        db_config_dwh_samara['port'],
        db_config_dwh_samara['db'],
    )


    engine_first_db = create_engine(connection_string, connect_args={'sslmode':'require'}) 
    engine_samara_db = create_engine(connection_string_samara, connect_args={'sslmode':'require'})
   
    print(dim_cars(engine_first_db, engine_samara_db))
    print(dim_clients(engine_first_db, engine_samara_db))
    print(dim_drivers(engine_first_db, engine_samara_db))
    print(waybill_fill(engine_samara_db, ftps_login, ftps_pass))
    print(fact_rides_fill(engine_first_db, engine_samara_db))
    print(fact_paymets_fill_func(engine_samara_db, ftps_login, ftps_pass))
    print(rep_drivers_violations_fill(engine_samara_db))
    print(rep_drivers_payments_fill(engine_samara_db))
    print(rep_drivers_overtime_fill(engine_samara_db))
    print(rep_clients_hist_fill(engine_samara_db))
    
    engine_first_db.dispose()
    engine_samara_db.dispose()