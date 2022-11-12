# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 12:54:04 2022

GlowByte Autumn Hack 2022
Team 8

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
#импорт библиотек
import pandas as pd
from datetime import timedelta

def fact_rides_fill(engine_first_db, engine_samara_db):
    #запросим последнюю дату обновления ДБ dwh_samara.dim_clients
    query = '''SELECT MAX(ride_arrival_dt) 
            FROM dwh_samara.fact_rides AS dc'''

    fact_rides = pd.read_sql_query(query, con=engine_samara_db)
    max_rides_dt = fact_rides['max'].to_list()[0]
    #если max_rides_dt не пустой - используем его в запросе к main.rides, иначе сформируем запрос без нее
    if max_rides_dt:
        #заказа машины и подачи отличаются - учтем это отличие с небольшим запасом
        max_rides_dt_ = max_rides_dt - timedelta(hours=1)
        query = f'''SELECT *
                    FROM (
                    SELECT ride,
                           movement_id ,
                           r.dt as issue_dt,
                           r.point_from as point_from_txt,
                           r.point_to as point_to_txt,
                           r.distance as distance_val,
                           r.price as price_amt,
                           r.client_phone as client_phone_num,
                           car_plate_num AS car_plate_num, 
                           event AS first_event,
                           lead(event, 1, NULL) over (partition by ride order by movement_id) as second_event,
                           lead(event, 2, NULL) over (partition by ride order by movement_id) as third_event,
                           m.dt as ride_arrival_dt,
                           lead(m.dt, 1, NULL) over (partition by ride order by movement_id) as ride_start_dt,
                           lead(m.dt, 2, NULL) over (partition by ride order by movement_id) as ride_end_dt
                    from main.movement m 
                    join
                    main.rides r on r.ride_id = m.ride
                    order by ride, movement_id) mr
                    WHERE NOT(second_event IS NULL AND third_event IS NULL OR 
                              first_event = 'BEGIN' AND second_event = 'END' AND third_event IS NULL OR 
                              first_event = 'READY' AND second_event = 'BEGIN' AND third_event IS NULL) AND
                              issue_dt > '{max_rides_dt_}';
            '''
    else:
        query = '''SELECT *
                    FROM (
                    SELECT ride,
                           movement_id ,
                           r.dt as issue_dt,
                           r.point_from as point_from_txt,
                           r.point_to as point_to_txt,
                           r.distance as distance_val,
                           r.price as price_amt,
                           r.client_phone as client_phone_num,
                           car_plate_num AS car_plate_num, 
                           event AS first_event,
                           lead(event, 1, NULL) over (partition by ride order by movement_id) as second_event,
                           lead(event, 2, NULL) over (partition by ride order by movement_id) as third_event,
                           m.dt as ride_arrival_dt,
                           lead(m.dt, 1, NULL) over (partition by ride order by movement_id) as ride_start_dt,
                           lead(m.dt, 2, NULL) over (partition by ride order by movement_id) as ride_end_dt
                    from main.movement m 
                    join
                    main.rides r on r.ride_id = m.ride
                    order by ride, movement_id) mr
                    WHERE NOT(second_event IS NULL AND third_event IS NULL OR 
                              first_event = 'BEGIN' AND second_event = 'END' AND third_event IS NULL OR 
                              first_event = 'READY' AND second_event = 'BEGIN' AND third_event IS NULL)
            '''
    #запрос данных из main.rides
    rides = pd.read_sql_query(query, con=engine_first_db) 
    
    #если max_rides_dt не пустой - используем его в запросе, иначе сформируем запрос без нее
    if max_rides_dt:
        #смена могла начаться раньше, поэтому увеличим диапазон на сутки
        max_rides_dt_ = max_rides_dt - timedelta(days=1)
        query = f'''SELECT *
                    FROM dwh_samara.fact_waybills
                    WHERE work_start_dt > '{max_rides_dt_}'
            '''
    else:
        query = '''SELECT *
                    FROM dwh_samara.fact_waybills
            '''
    
    #запрос данных из dwh_samara.fact_waybills
    fact_waybill = pd.read_sql_query(query, con=engine_samara_db)
    
    #удалим лисние пробелы в car_plate_num
    rides['car_plate_num'] = rides['car_plate_num'].apply(lambda x: x.strip())
    
    def find_driver(dt, car_plate_num):
        '''
        Parameters
        ----------
        dt : datetime64[ns]
            время подачи ватомобиля
        car_plate_num : str
            номер автомобиля

        Returns
        -------
        str or None
            для каждой поездки находим водителя по путевому листу
            отбираем по номеру и дате подачи машины.

        '''
        try:
            return fact_waybill.loc[(fact_waybill['car_plate_num'] == car_plate_num) &
                                ((fact_waybill['work_start_dt'] <= dt) &
                                 (fact_waybill['work_end_dt'] >= dt))
                               ]['driver_pers_num'].values[0]
        except:
            return None
        
    #заполним driver_pers_num водителей
    for i in range(len(rides)):
        rides.loc[i, 'driver_pers_num'] = find_driver(rides.loc[i, 'ride_arrival_dt'], rides.loc[i, 'car_plate_num'])
        
    #сформируем строку для отправки данных в запросе INSERT INTO 
    str_to_append = ''
    iterator = zip(rides['point_from_txt'],
                   rides['point_to_txt'],
                   rides['distance_val'], 
                   rides['price_amt'], 
                   rides['client_phone_num'],
                   rides['driver_pers_num'],
                   rides['car_plate_num'],
                   rides['first_event'],
                   rides['second_event'],
                   rides['third_event'], 
                   rides['ride_arrival_dt'],
                   rides['ride_start_dt'],
                   rides['ride_end_dt']
                  )
    for point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, driver_pers_num, \
        car_plate_num, first_event, second_event, third_event, ride_arrival_dt, ride_start_dt, ride_end_dt in iterator:
        if (second_event == 'CANCEL'):
            str_to_append  = (str_to_append + f"('{point_from_txt}', '{point_to_txt}', '{distance_val}', '{price_amt}', '{client_phone_num}', "+
                             f"'{driver_pers_num}', '{car_plate_num}', '{ride_arrival_dt}', NULL, '{ride_start_dt}'),"
                             )
        else:
            str_to_append  = (str_to_append + f"('{point_from_txt}', '{point_to_txt}', '{distance_val}', '{price_amt}','{client_phone_num}',"+
                             f"'{driver_pers_num}', '{car_plate_num}', '{ride_arrival_dt}', '{ride_start_dt}','{ride_end_dt}'),"
                             )
            
    if (len(str_to_append)>0):
        query ='INSERT INTO dwh_samara.fact_rides (point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt) VALUES '
        query = query + str_to_append
        query = query[:-1]
        engine_samara_db.execute(query)
       
    #уберем дубликаты
    query = '''
        delete from fact_rides 
        where ride_id in (
                select ride_id
                from (select ride_id, row_number() over (partition by ride_arrival_dt, ride_end_dt, car_plate_num) as diplacate
                from fact_rides fr ) fr2
                where diplacate > 1);
    '''
    engine_samara_db.execute(query)
    
    return 'Fact_rides filling is finished'

if __name__ == "__main__":
    pass

