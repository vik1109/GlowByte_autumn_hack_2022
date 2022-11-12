# -*- coding: utf-8 -*-
"""
Created on Sat Oct 29 17:00:26 2022

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""

import pandas as pd

def rep_drivers_violations_fill(engine_samara_db):
    #запросим максимальное значение ride в ДБ rep_drivers_violations
    query = '''SELECT MAX(ride) 
                FROM dwh_samara.rep_drivers_violations AS rdv'''
    
    max_ride_id = pd.read_sql_query(query, con=engine_samara_db)
    
    #запросим все поездки, где водители превышали скорость 85 км час
    max_ride_id_num = max_ride_id['max'].to_list()[0]
    if max_ride_id_num:
        query = f"""select driver_pers_num, ride_id, ROUND((distance_val/second_ * 3600)::numeric, 2) as speed
                    from(
                        select driver_pers_num , ride_id , distance_val, 
                               (date_part('hour', (ride_end_dt - ride_start_dt))*3600
                               + date_part('minute', (ride_end_dt - ride_start_dt))*60
                               + date_part('second', (ride_end_dt - ride_start_dt))) as second_
                        from fact_rides
                        where not ride_start_dt is null AND  ride_id > {max_ride_id_num}) fr
                    where ceil(distance_val/second_ * 3600) > 85
        """
    else:
        query = """select driver_pers_num, ride_id, ROUND((distance_val/second_ * 3600)::numeric, 2) as speed
                    from(
                        select driver_pers_num , ride_id , distance_val, 
                               (date_part('hour', (ride_end_dt - ride_start_dt))*3600
                               + date_part('minute', (ride_end_dt - ride_start_dt))*60
                               + date_part('second', (ride_end_dt - ride_start_dt))) as second_
                        from fact_rides
                        where not ride_start_dt is null ) fr
                    where ceil(distance_val/second_ * 3600) > 85
        """
    avg_speed = pd.read_sql_query(query, con=engine_samara_db)
    
    #сформируем строку для отправки данных в запросе INSERT INTO 
    str_to_append = ''
    iterator = zip(avg_speed['driver_pers_num'],
                   avg_speed['ride_id'],
                   avg_speed['speed']
                  )
    for driver_pers_num, ride_id, speed in iterator:
        str_to_append  = (str_to_append + f"('{driver_pers_num}', '{ride_id}', '{speed}', NULL),")
        
    #добавим в БД новые значения и обновим занчение violations_cnt
    if (len(str_to_append)>0):
        query ='INSERT INTO dwh_samara.rep_drivers_violations (personnel_num, ride, speed, violations_cnt) VALUES '
        query = query + str_to_append
        query = query[:-1]
        engine_samara_db.execute(query)
        
        query = """
            update dwh_samara.rep_drivers_violations d
            set violations_cnt = r1.shift
            from (select ride , (row_number() over (partition by personnel_num order by ride) - 1) as shift from rep_drivers_violations) r1
            where violations_cnt is null and d.ride = r1.ride
        """
        #отправим запрос на обновление к БД dwh_samara.rep_drivers_violations
        engine_samara_db.execute(query)
    
    return "Rep_drivers_violations filling Done"

if __name__ == "__main__":
    pass
    
    
    