# -*- coding: utf-8 -*-
"""
Created on Sat Oct 29 20:54:19 2022

@author: https://github.com/vik1109/
         https://github.com/Den1079
"""
import pandas as pd

def rep_drivers_payments_fill(engine_samara_db):
    #запросим максимальное значение ride в ДБ rep_drivers_violations
    query = '''SELECT MAX(report_dt) 
                FROM dwh_samara.rep_drivers_payments AS rdv'''
    
    max_report_dt = pd.read_sql_query(query, con=engine_samara_db)
    
    max_report_dt_num = max_report_dt['max'].to_list()[0]
    if max_report_dt_num:
        query = f''' SELECT driver_pers_num, DATE(ride_start_dt) as report_dt,
                          sum(distance_val) as distance_val,
                          sum(price_amt) as price_amt      
                    FROM fact_rides
                    WHERE ride_start_dt IS NOT NULL
                    GROUP BY driver_pers_num, DATE(ride_start_dt)
                    having DATE(ride_start_dt) < CURRENT_DATE AND DATE(ride_start_dt) > '{max_report_dt_num}'
                    '''
    else:
        query = ''' SELECT driver_pers_num, DATE(ride_start_dt) as report_dt,
                          sum(distance_val) as distance_val,
                          sum(price_amt) as price_amt      
                    FROM fact_rides
                    WHERE ride_start_dt IS NOT NULL
                    GROUP BY driver_pers_num, DATE(ride_start_dt)
                    having DATE(ride_start_dt) < CURRENT_DATE
                    '''
    new_reports = pd.read_sql_query(query, con=engine_samara_db)
    #print(query)
    if len(new_reports) > 0:
        query = '''
                SELECT personnel_num,
                       last_name,
                       first_name,
                       middle_name,
                       card_num
                FROM dim_drivers
                WHERE deleted_flag = 'N'
                '''
        dim_drivers = pd.read_sql_query(query, con=engine_samara_db)
    
        new_reports['amount'] = round(new_reports['price_amt'] -
                            new_reports['price_amt']*0.2 -
                            new_reports['distance_val'] * 47.26 * 7/ 100 -
                            new_reports['distance_val'] * 5, 2)
        
        df_rep_drivers_payments = new_reports.merge(dim_drivers, left_on='driver_pers_num', 
                                                    right_on='personnel_num', how = 'left')
        
        #сформируем строку для отправки данных в запросе INSERT INTO 
        str_to_append = ''
        iterator = zip(df_rep_drivers_payments['personnel_num'],
                       df_rep_drivers_payments['last_name'],
                       df_rep_drivers_payments['first_name'],
                       df_rep_drivers_payments['middle_name'],
                       df_rep_drivers_payments['card_num'],
                       df_rep_drivers_payments['amount'],
                       df_rep_drivers_payments['report_dt']
                      )
        for personnel_num, last_name, first_name, middle_name, card_num, amount, report_dt in iterator:
            str_to_append  = (str_to_append + f"('{personnel_num}', '{last_name}', '{first_name}', '{middle_name}', "+
                              f"'{card_num}', '{amount}', '{report_dt}'),"
                             )
        
        if (len(str_to_append)>0):
            query ='INSERT INTO dwh_samara.rep_drivers_payments (personnel_num, last_name, first_name, middle_name, card_num, amount, report_dt) VALUES '
            query = query + str_to_append
            query = query[:-1]
            engine_samara_db.execute(query)
            
    return 'Rep_drivers_payments filling done'

if __name__ == "__main__":
    pass