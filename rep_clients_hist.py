# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 20:45:25 2022

@@author: https://github.com/vik1109/
         https://github.com/Den1079
"""

def rep_clients_hist_fill(engine_samara_db):
    '''
    Шаг 1. Чистим данные.
    Шаг 2. Изначально объединяем diM_clients и fact_payments (inner join)
        при объединении учитываем время свершения транзакции и номер карты
    Шаг 3. Получаем 3 таблицы:
        1. Все успешно оплаченные поездки. fact_rides объединяем с предыдущей таблицей (inner join)
            по полям fr.price_amt = c.transaction_amt , fr.client_phone_num = c.phone_num,
            fr.ride_arrival_dt < c.transaction_dt
        2. Все отменённые поездки
        3. Все не оплаченные поездки. fact_rides объединяем с предыдущей таблицей (right join)
            по полям fr.price_amt = c.transaction_amt , fr.client_phone_num = c.phone_num,
            fr.ride_arrival_dt < c.transaction_dt при условии, что where transaction_dt is null
        Через union объединяем их
    Шаг 4. Готовим технические столбцы необходимые нам для интересующих нас подсчетов и
        столбцы dt_interval и id_row, которые помогут нам выявить дубли (id_row больше 1 только при одинаковых
                                                                         номерах транзакций)
    Шаг 5. Формируем итоговую таблицу обрабатываем дубликаты. Добавляем 
        столбец clien_id (нумеруюем все данные отсортировав по дате, при повторных заполнениях
                          нумерация останется постоянной)
    Шаг 6. Записываем данные в БД

    '''
    query = '''
    TRUNCATE TABLE dwh_samara.rep_clients_hist;
    insert into dwh_samara.rep_clients_hist (phone_num, rides_cnt, start_dt, deleted_flag, cancelled_cnt, spent_amt, debt_amt, end_dt, client_id)
    select client_phone_num, rides_cnt, ride_arrival_dt as start_dt, 'N' as deleted_flag,
           SUM(cancelled) over (partition by client_phone_num order by ride_start_dt) as cancelled_cnt,
           SUM(transaction_amt) over (partition by client_phone_num order by ride_start_dt) as spent_amt,
           SUM(case when (not ride_start_dt is null and transaction_amt = 0) then price_amt else 0 end) over (partition by client_phone_num order by ride_start_dt) as debt_amt,
           LEAD(ride_arrival_dt, 1, '2222-12-31 23:59:59.000') over (partition by client_phone_num order by ride_arrival_dt) as end_dt,
           ROW_NUMBER() over (ORDER BY ride_arrival_dt) AS client_id
    from (select ride_id, price_amt,
    	       client_phone_num, 
    	       ride_arrival_dt,
    	       ride_start_dt, 
    	       case when (ride_start_dt is null) then 1 else 0 end as cancelled,
    	       ride_end_dt, 
    	       case when ride_start_dt is null then null else card_num end as card_num, 
    	       case when ride_start_dt is null then null else transaction_dt end as transaction_dt, 
    	       transaction_id,
    	       case when ride_start_dt is null or transaction_amt is null then 0 else transaction_amt end as transaction_amt,
    	       row_number() over (partition by client_phone_num order by ride_start_dt) as rides_cnt,
               transaction_dt - ride_arrival_dt AS dt_interval,
               row_number() over (partition by transaction_id order by ride_start_dt) as id_row
    	from (select ride_id, price_amt, client_phone_num, ride_arrival_dt, ride_start_dt, ride_end_dt, card_num, transaction_dt, transaction_id, transaction_amt
    			from fact_rides fr 
    			inner join( select phone_num, min(start_dt) as start_dt,
    		                dc.card_num, max(end_dt) as end_dt, 
    		                fp.transaction_amt as transaction_amt, 
    		                fp.transaction_dt,
    		                fp.transaction_id
    			from dim_clients dc
    			inner join fact_payments fp on dc.card_num = fp.card_num and
    		                                   dc.start_dt < fp.transaction_dt and
    		                                   fp.transaction_dt < end_dt
    			group by phone_num, dc.card_num, fp.transaction_amt, fp.transaction_dt, fp.transaction_id) c on fr.price_amt = c.transaction_amt and
    			                                                                              fr.client_phone_num = c.phone_num and
    		                                                                                  fr.ride_arrival_dt < c.transaction_dt  
    			union 
    			select ride_id, price_amt, client_phone_num, ride_arrival_dt, ride_start_dt, ride_end_dt,  null, null, null, null
    			from fact_rides fr
    			where ride_start_dt is null
    			union 
    			select ride_id, price_amt, client_phone_num, ride_arrival_dt, ride_start_dt, ride_end_dt, card_num, transaction_dt, transaction_id, transaction_amt
    				from fact_rides fr 
    				left join( select phone_num, min(start_dt) as start_dt,
    			                dc.card_num, max(end_dt) as end_dt, 
    			                fp.transaction_amt as transaction_amt, 
    			                fp.transaction_dt,
    			                fp.transaction_id
    				from dim_clients dc
    				inner join fact_payments fp on dc.card_num = fp.card_num and
    			                                   dc.start_dt < fp.transaction_dt and
    			                                   fp.transaction_dt < end_dt
    				group by phone_num, dc.card_num, fp.transaction_amt, fp.transaction_dt, fp.transaction_id) c on fr.price_amt = c.transaction_amt and
    				                                                                              fr.client_phone_num = c.phone_num and
    			                                                                                  fr.ride_arrival_dt < c.transaction_dt
    			where transaction_dt is null) t1) t2
    where not(id_row = 2 and dt_interval < interval '1 hour');
    
    '''
    engine_samara_db.execute(query)
    
    return 'Rep_clients_hist filling done'