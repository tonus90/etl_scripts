/* 3. Сформировать SQL-запрос: вывести callsign, registration, количество рейсов за всё время для callsign = ETD18U 
с разбивкой по каждому из registration (бортовому номеру самолета) */

select callsign, registration , count(*) as flight_cnt
from opensky o 
where callsign = 'ETD18U' 
group by callsign, registration;