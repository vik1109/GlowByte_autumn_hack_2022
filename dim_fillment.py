# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 17:09:19 2022

GlowByte Autumn Hack 2022
Team 8

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
#импорт библиотек
import pandas as pd
from datetime import datetime


def dim_cars(engine_first_db, engine_samara_db):
    #запросим последнюю дату обновления ДБ dwh_samara.dim_cars
    query = '''SELECT MAX(start_dt) 
                FROM dwh_samara.dim_cars AS dc'''
    
    dim_cars = pd.read_sql_query(query, con=engine_samara_db)
    #если дата есть - используем ее в запросе к main.car_pool, иначе сформируем запрос без нее
    max_start_cars = dim_cars['max'].to_list()[0]
    if max_start_cars:
        query = f'''SELECT *
            FROM main.car_pool
            WHERE update_dt > '{max_start_cars}';
            '''
    else:
        query = '''SELECT * 
            FROM main.car_pool;
            '''
    #запрос к БД
    cars = pd.read_sql_query(query, con=engine_first_db)
    
    #обработка столбцов, подготовка к отправке
    cars = cars.rename(columns = {'plate_num': 'plate_num',
                                          'model': 'model_name',
                                          'update_dt': 'start_dt',
                                          'revision_dt': 'revision_dt',
                                          'finished_flg': 'deleted_flag'
                                         })
    cars = cars.drop('register_dt', axis = 1)
    cars['end_dt'] = datetime.strptime("31/12/2222 23:59:59", "%d/%m/%Y %H:%M:%S")
    
    #подготовка VALUES для отправки
    str_to_append = ''
    iterator = zip(cars['plate_num'],
                   cars['start_dt'],
                   cars['model_name'], 
                   cars['revision_dt'], 
                   cars['deleted_flag'],
                   cars['end_dt'])
    for plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt in iterator:
        str_to_append  = str_to_append + f"('{plate_num}', '{start_dt}', '{model_name}', '{revision_dt}', '{deleted_flag}', '{end_dt}'),"
    
    #если есть что отправлять готовим INSERN INTO и отправляем данные
    if (len(str_to_append)>0):
        query ='INSERT INTO dwh_samara.dim_cars (plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt) VALUES '
        query = query + str_to_append
        query = query[:-1]
        engine_samara_db.execute(query)
    
    #обновим end_dt и deleted_flag для строк где deleted_flag = 'N'
    query = '''UPDATE dwh_samara.dim_cars as dc1
            SET end_dt = subquery.new_end_dt 
            FROM(
                select plate_num, start_dt,
                LEAD(start_dt, 1, '2222-12-31 23:59:59.000') over (partition by plate_num order by start_dt) as new_end_dt
                from dwh_samara.dim_cars
                where deleted_flag = 'N') as subquery
            WHERE dc1.plate_num = subquery.plate_num and dc1.start_dt = subquery.start_dt;
            '''
    #отправим запрос на обновление к БД dwh_samara.dim_clients
    engine_samara_db.execute(query)
    
    return 'dim_cars Done'

def dim_drivers(engine_first_db, engine_samara_db):
    #запросим последнюю дату обновления ДБ dwh_samara.dim_drivers
    query = '''SELECT MAX(start_dt) 
                FROM dwh_samara.dim_drivers AS dc'''
    
    dim_drivers = pd.read_sql_query(query, con=engine_samara_db)
    
    #если дата есть - используем ее в запросе к main.drivers, иначе сформируем запрос без нее
    max_start_drivers = dim_drivers['max'].to_list()[0]
    if max_start_drivers:
        query = f'''SELECT *
            FROM main.drivers
            WHERE update_dt > '{max_start_drivers}';
            '''
    else:
        query = '''SELECT * FROM main.drivers;'''
    #запрос к БД main.drivers
    drivers = pd.read_sql_query(query, con=engine_first_db)
    
    #подготовка таблицы к загрузке в БД
    drivers['driver_valid_to'] = pd.to_datetime(drivers['driver_valid_to'])
    drivers['birth_dt'] = pd.to_datetime(drivers['birth_dt'])
    drivers = drivers.rename(columns = {'driver_license': 'driver_license_num',
                                        'driver_valid_to': 'driver_license_dt',
                                        'update_dt': 'start_dt',
                                        'index': 'personnel_num'
                                       })
    
    drivers['card_num'] = drivers['card_num'].replace(' ', '')
    drivers['deleted_flag'] = 'N'
    drivers['end_dt'] = datetime.strptime("31/12/2222 23:59:59", "%d/%m/%Y %H:%M:%S")
    drivers['personnel_num'] = drivers['driver_license_num'].apply(lambda x: ''.join(x.split()))
    
    #подготовка VALUES для отправки в БД
    str_to_append = ''
    iterator = zip(drivers['driver_license_num'],
                   drivers['first_name'],
                   drivers['last_name'], 
                   drivers['middle_name'], 
                   drivers['driver_license_dt'],
                   drivers['card_num'],
                   drivers['start_dt'], 
                   drivers['birth_dt'],
                   drivers['deleted_flag'], 
                   drivers['end_dt'],
                   drivers['personnel_num']
                  )
    for driver_license_num, first_name, last_name, middle_name, driver_license_dt, card_num, start_dt, birth_dt, deleted_flag, end_dt, personnel_num in iterator:
        str_to_append  = (str_to_append + f"('{driver_license_num}', '{first_name}', '{last_name}', " +
                          f"'{middle_name}','{driver_license_dt}', '{card_num}', '{start_dt}', " +
                          f"'{birth_dt}', '{deleted_flag}', '{end_dt}', '{personnel_num}'),")
    #если данные для отпроавки есть дорабатываем строку и отправляем запрос на добавление    
    if (len(str_to_append)>0):
        query ='INSERT INTO dwh_samara.dim_drivers (driver_license_num, first_name, last_name, middle_name, driver_license_dt, card_num, start_dt, birth_dt, deleted_flag, end_dt, personnel_num) VALUES '
        query = query + str_to_append
        query = query[:-1]
        engine_samara_db.execute(query)
    
    #обновим end_dt и deleted_flag для строк где deleted_flag = 'N'
    query = '''UPDATE dwh_samara.dim_drivers as dc1
            SET end_dt = subquery.new_end_dt 
            FROM(
                select personnel_num, start_dt,
                LEAD(start_dt, 1, '2222-12-31 23:59:59.000') over (partition by personnel_num order by start_dt) as new_end_dt
                from dwh_samara.dim_drivers
                where deleted_flag = 'N') as subquery
            WHERE dc1.personnel_num = subquery.personnel_num and dc1.start_dt = subquery.start_dt;
            
            update dwh_samara.dim_drivers 
                set deleted_flag = case when end_dt = '2222-12-31 23:59:59.000'then 'N' else 'Y' end
                where deleted_flag = 'N';
            '''
    #отправим запрос на обновление к БД dwh_samara.dim_drivers
    engine_samara_db.execute(query)
    
    return 'dim_drivers Done'

def dim_clients(engine_first_db, engine_samara_db):
    #запросим последнюю дату обновления ДБ dwh_samara.dim_clients
    query = '''SELECT MAX(start_dt) 
                FROM dwh_samara.dim_clients AS dc'''
    
    dim_clients = pd.read_sql_query(query, con=engine_samara_db)
    
    #если дата есть - используем ее в запросе к main.rides, иначе сформируем запрос без нее
    max_start_clients = dim_clients['max'].to_list()[0]
    if max_start_clients:
        query = f'''SELECT *
            FROM main.rides
            WHERE dt > '{max_start_clients}';
            '''
    else:
        query = '''SELECT * FROM main.rides;'''
    #запрос данных из main.rides
    rides = pd.read_sql_query(query, con=engine_first_db) 
    
    #проведем преобразования над rides, подготовим данны для загрузки в dwh_samara.dim_clients
    cliens = rides[['dt', 'client_phone', 'card_num']]
    cliens.head()
    cliens = cliens.rename(columns = {
        'client_phone': 'phone_num',
        'dt':'start_dt'
    })
    cliens['deleted_flag'] = 'N'
    cliens['end_dt'] = datetime.strptime("31/12/2222 23:59:59", "%d/%m/%Y %H:%M:%S")
    cliens['card_num'] = cliens['card_num'].replace(' ', '')
    
    #сформируем строку для отправки данных в запросе INSERT INTO dwh_samara.dim_clients
    str_to_append = ''
    iterator = zip(cliens['start_dt'],
                   cliens['phone_num'],
                   cliens['card_num'], 
                   cliens['deleted_flag'], 
                   cliens['end_dt']
                  )
    for start_dt, phone_num, card_num, deleted_flag, end_dt in iterator:
        str_to_append  = (str_to_append + f"('{start_dt}', '{phone_num}', '{card_num}', '{deleted_flag}','{end_dt}'),")
    
    #если данные str_to_append не пустые - завершим запрос и отправим данные в БД dwh_samara.dim_clients
    if (len(str_to_append)>0):
        query ='INSERT INTO dwh_samara.dim_clients (start_dt, phone_num, card_num, deleted_flag, end_dt) VALUES '
        query = query + str_to_append
        query = query[:-1]
        engine_samara_db.execute(query)
    
    #обновим end_dt и deleted_flag для строк где deleted_flag = 'N'
    query = '''UPDATE dim_clients as dc1
            SET end_dt = subquery.new_end_dt 
            FROM(
                select phone_num, start_dt,
                LEAD(start_dt, 1, '2222-12-31 23:59:59.000') over (partition by phone_num order by start_dt) as new_end_dt
                from dim_clients
                where deleted_flag = 'N') as subquery
            WHERE dc1.phone_num = subquery.phone_num and dc1.start_dt = subquery.start_dt;
            
            update dim_clients 
                set deleted_flag = case when end_dt = '2222-12-31 23:59:59.000'then 'N' else 'Y' end
                where deleted_flag = 'N';
            '''
    #отправим запрос на обновление к БД dwh_samara.dim_clients
    engine_samara_db.execute(query)
    
    return 'dim_clients Done'
    
    
    
if __name__ == "__main__":
    pass