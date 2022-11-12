# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 15:52:21 2022

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
import pandas as pd

def rep_drivers_overtime_fill(engine_samara_db):
    #запросим максимальную заполненную заду в БД
    query = ''' SELECT MAX(start_dt)
                FROM rep_drivers_overtime
    '''
    max_rep_drivers_overtime = pd.read_sql_query(query, con=engine_samara_db)
    
    #если max_rep_drivers_overtime_dt есть - формируем запрос с учетом max_rep_drivers_overtime_dt, иначе без
    max_rep_drivers_overtime_dt = max_rep_drivers_overtime['max'].to_list()[0]
    if max_rep_drivers_overtime_dt:
        query = f'''
            SELECT driver_pers_num,
                   work_start_dt, 
                   working_hours
            FROM (SELECT *,
                        CASE
                            WHEN next_end_dt > (work_start_dt +'1 day'::interval) THEN j.working_hours_1 - (next_end_dt - (work_start_dt +'1 day'::interval))
                            ELSE  working_hours_1
                          END as working_hours
                 FROM (SELECT *,       
                             CASE
                                 WHEN next_work_start < (work_start_dt +'1 day'::interval) THEN i.sum_hour + (next_end_dt - next_work_start) 
                               END as working_hours_1
                     FROM (SELECT driver_pers_num,
                                  work_start_dt,
                                  work_end_dt,
                                  LEAD(work_start_dt,1, NULL) OVER (PARTITION BY driver_pers_num ORDER BY work_start_dt) as next_work_start,
                                  LEAD(work_end_dt,1, NULL) OVER (PARTITION BY driver_pers_num) as next_end_dt, 
                                  work_end_dt - work_start_dt as sum_hour 
                           FROM fact_waybills
                           WHERE work_start_dt > '{max_rep_drivers_overtime_dt}') as i
                    WHERE i.next_work_start is not null or (i.next_work_start is null and i.sum_hour > '08:00:00')) AS j) as k
            WHERE working_hours is not null and working_hours > '08:00:00' AND work_start_dt < CURRENT_DATE - '1 day'::interval
        '''
    else:
        query = '''
            SELECT driver_pers_num,
                   work_start_dt, 
                   working_hours
            FROM (SELECT *,
                        CASE
                            WHEN next_end_dt > (work_start_dt +'1 day'::interval) THEN j.working_hours_1 - (next_end_dt - (work_start_dt +'1 day'::interval))
                            ELSE  working_hours_1
                          END as working_hours
                 FROM (SELECT *,       
                             CASE
                                 WHEN next_work_start < (work_start_dt +'1 day'::interval) THEN i.sum_hour + (next_end_dt - next_work_start) 
                               END as working_hours_1
                     FROM (SELECT driver_pers_num,
                                  work_start_dt,
                                  work_end_dt,
                                  LEAD(work_start_dt,1, NULL) OVER (PARTITION BY driver_pers_num ORDER BY work_start_dt) as next_work_start,
                                  LEAD(work_end_dt,1, NULL) OVER (PARTITION BY driver_pers_num) as next_end_dt, 
                                  work_end_dt - work_start_dt as sum_hour 
                           FROM fact_waybills) as i
                    WHERE i.next_work_start is not null or (i.next_work_start is null and i.sum_hour > '08:00:00')) AS j) as k
            WHERE working_hours is not null and working_hours > '08:00:00' AND work_start_dt < CURRENT_DATE - '1 day'::interval
        '''
    rep_drivers_overtime = pd.read_sql_query(query, con=engine_samara_db)
    #сформируем строку для отправки данных в запросе INSERT INTO если таблица не пустая
    if len(rep_drivers_overtime)>0:
        str_to_append = ''
        iterator = zip(rep_drivers_overtime['driver_pers_num'],
                       rep_drivers_overtime['work_start_dt'],
                       rep_drivers_overtime['working_hours']
                      )
        for driver_pers_num, work_start_dt, working_hours in iterator:
            str_to_append  = (str_to_append + f"('{driver_pers_num}', '{work_start_dt}', '{str(working_hours)[-8:-3]}'),")
        
        if (len(str_to_append)>0):
            query ='INSERT INTO dwh_samara.rep_drivers_overtime (personnel_num, start_dt, work_time) VALUES '
            query = query + str_to_append
            query = query[:-1]
            engine_samara_db.execute(query)
    
    return 'Rep_drivers_overtime filling done'
    
if __name__ == "__main__":
    pass