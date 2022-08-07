-- Подготовка

-- На вход я получил данные в формате .csv
-- Чтобы прописать и протестить SQL скрипты, я поднял кластер Clickhouse (мой любимый на данный момент SQL'ный диалект), создал таблицу ORDERS со струтурой присланного csv, и загнал туда все данные
-- Ниже приведены скрипты, которые позволили мне это сделать

-- Создадаю датабейз под тестовое задание 
CREATE DATABASE IF NOT EXISTS test

-- Создаю структуру таблицы
CREATE TABLE test.orders
(
    ORDER_ID UInt32,
    DATE_CREATED Date,
    ORDER_STATE String,
    PRODUCT_ID UInt32,
    PRODUCT_ITEM_ID UInt32,
    PRODUCT_NM String,
    PRODUCT_STATUS String,
    BRAND_NM String,
    PRODUCT_GENDER_NM String,
    PRODUCT_COLOR_ID String, 
    PRODUCT_COLOR_NM String,
    PRODUCT_SIZE_NM String, 
    PRODUCT_COUNTRY_NM String, 
    PRODUCT_COLLECTION_NM String,       
    PRODUCT_COMPOSITION_NM String, 
    PRODUCT_MATERIAL_NM String,
    PRODUCT_MATERIAL_LINING_NM String, 
    PRODUCT_MATERIAL_INNER_NM String,
    PRODUCT_MATERIAL_TOP_NM String, 
    PRODUCT_MATERIAL_SOLE_NM String,
    PRODUCT_CATEGORY1 String, 
    PRODUCT_CATEGORY2 String, 
    PRODUCT_CATEGORY3 String,
    PRODUCT_CATEGORY4 String, 
    PRODUCT_CATEGORY5 String, 
    PRODUCT_CATEGORY6 String,
	PRODUCT_QTY String, 
	PRODUCT_BASE_PRICE_AMT Float
)
ENGINE = MergeTree()

-- Сам инсерт я выполнил через терминал следующей командой:
-- ./clickhouse client --query='INSERT INTO test.orders FORMAT CSV' < csv/input.csv 
-- Где input.csv - та же таблица orders с убранными названиями столбцов

----------------------------------------------
------------- Само выполнение ----------------
----------------------------------------------

-- Task 1
-- 1.	Вычислить среднюю выручку чека и среднее количество проданных товаров на 1 чек

with t1 as (
select ORDER_ID, SUM(toInt8(PRODUCT_QTY) * PRODUCT_BASE_PRICE_AMT) as revenue, SUM(toInt8(PRODUCT_QTY)) as goods_amount 
from test.my_first_table
WHERE ORDER_STATE = 'checkout'
group by ORDER_ID
)
SELECT  AVG(revenue) as mean_revenue, AVG(goods_amount) as mean_amount
from t1


-- Task 2
-- 2.	Вычислить топ 10 самых продаваемых категорий товаров по объему продаж для женщин

/*
 Возникает характерный вопрос: о какой из 6 категорий речь
 2-я слишком грубая, как мы увидим в первом скрипте 
 3-я более объемлющая, кажется, она подходит лучше
 4-я и следующие содержат больше пропусков - рискну предположить, что они являются усточняющими
 
 План:
 Рассматриваем 2-ю, 3-ю по отдельности (1-й, 2-й скрипты)
 Рассматриваем их же совместно (3-й скрипт)  
 */
with t1 as (
	select PRODUCT_CATEGORY2 as category_name, SUM(toInt8(PRODUCT_QTY)) as goods_amount 
	from test.my_first_table
	WHERE ORDER_STATE = 'checkout'
	and PRODUCT_GENDER_NM = 'Женский'
	group by PRODUCT_CATEGORY2
	order by goods_amount DESC 
)
SELECT *--category_name
from t1
limit 10

-- Есть одна категория no_name. Можно убрать из рассмотрения пустые категории, но я считаю, это в некотором смысле важная категория
-- А еще спешу отметить, что для этой категории топ-10 избыточен - их и так в женском случае 10. Тем не менее, тоже показатель

with t1 as (
	select PRODUCT_CATEGORY3 as category_name, SUM(toInt8(PRODUCT_QTY)) as goods_amount 
	from test.my_first_table
	WHERE ORDER_STATE = 'checkout'
	and PRODUCT_GENDER_NM = 'Женский'
	group by PRODUCT_CATEGORY3
	order by goods_amount DESC 
)
SELECT *--category_name
from t1
limit 10


with t1 as (
	select PRODUCT_CATEGORY2 as category_name, PRODUCT_CATEGORY3 as category_name2, SUM(toInt8(PRODUCT_QTY)) as goods_amount 
	from test.my_first_table
	WHERE ORDER_STATE = 'checkout'
	and PRODUCT_GENDER_NM = 'Женский'
	group by PRODUCT_CATEGORY2, PRODUCT_CATEGORY3 
	order by goods_amount DESC 
)
SELECT *--category_name
from t1
limit 10

-- Я затрудняюсь ответить, какая из выборок более информативная, так как уверен, что это зависит от конечной цели

-- Еще посчитал информативным вывести таблицу по категориям 2, в каждой из которых посчитаны топ-10 категорий 3 внутри 2-й.
-- Звучит запутанно, зато ниже все понятно
  
with t1 as (
	select PRODUCT_CATEGORY2 as category_name, PRODUCT_CATEGORY3 as category_name2, 
	SUM(toInt8(PRODUCT_QTY)) over (partition by PRODUCT_CATEGORY2, PRODUCT_CATEGORY3 ) as goods_amount_23,
	SUM(toInt8(PRODUCT_QTY)) over (partition by PRODUCT_CATEGORY2 ) as  goods_amount_2
	from test.my_first_table
	WHERE ORDER_STATE = 'checkout'
	and PRODUCT_GENDER_NM = 'Женский'
	--group by PRODUCT_CATEGORY2, PRODUCT_CATEGORY3 
	order by goods_amount_2 DESC , goods_amount_23 DESC
	limit 1 by PRODUCT_CATEGORY2, PRODUCT_CATEGORY3
)
SELECT *--category_name
from t1
order by goods_amount_2 DESC, goods_amount_23 DESC 
limit 10 by category_name


-- Task3 
-- 3.	Вывести наименее загруженные дни работы интернет магазина (количество заказов < 50% от среднего количества заказов в день)
/*
 Снова я попытался трактовать задачу двояко - и у меня получилось: 
 Что значит "нагруженные дни"? 
 Возможно, речь о днях недели, когда никто ничего не заказывает по пн, например?
 А, может, следует понимать буквально - дни за историю магазина, когда была просадка по продажам
 Разберемся ниже
 */

-- 	среднеe количества заказов в день (Спойлер: их 800)
with days_of_order as 
		(select DATE_CREATED as order_day, count(DISTINCT ORDER_ID) as goods_amount 
		from test.my_first_table
		WHERE 1=1
		GROUP by order_day
		),
avg_per_day as
(	
	SELECT avg(goods_amount) as avg_per_day
	from days_of_order
)
select *
from avg_per_day


-- Среднее по дням недели
with days_of_order as 
	(select DATE_CREATED as order_day, count(DISTINCT ORDER_ID) as goods_amount 
	from test.my_first_table
	WHERE 1=1
	GROUP by order_day
	)
SELECT toDayOfWeek(order_day) as day_of_week, AVG(goods_amount) as goods_amount_pdw  
from days_of_order
group by day_of_week
order by goods_amount_pdw DESC 

-- Ну дальше нас ожидал божественный код с выводом дней недели, в которые продажи стабильно проседают на 50%, но какой смысл: очевидно, таковых не имеется. Божественный код поэтому я вам не покажу  

-- Так что теперь просто посмотрим, есть ли дни, в которые заказов < 50% от среднего

with days_of_order as 
		(select DATE_CREATED as order_day, count(DISTINCT ORDER_ID) as goods_amount 
		from test.my_first_table
		WHERE 1=1
		GROUP by order_day
		),
avg_per_day as
(	
	SELECT avg(goods_amount) as avg_per_day
	from days_of_order
)
select *
from days_of_order
where days_of_order.goods_amount <= 0.5 * (SELECT * from avg_per_day)

-- Не похоже. 
-- Можем поискать еще дни, в которые просадки просели относительно среднего в его конкретный день недели. Но следующий скрипт показывает, что это также не даст результатов

select DATE_CREATED as order_day, count(DISTINCT ORDER_ID) as goods_amount 
from test.my_first_table
WHERE 1=1
GROUP by order_day
ORDER by goods_amount


-- Также пытался применить эту логику к только оплаченным заказам (хотя в условии об этом ни слова, и правда, может, нас интересует именно нагруженность на сервер), но результат все равно пуст



-- 4.	Вывести самый продаваемый товар и бренд по количеству проданных товаров для каждого значения категории 4.

SELECT PRODUCT_CATEGORY4, PRODUCT_ID, BRAND_NM, sum(toInt8(PRODUCT_QTY)) as total_sum
from test.my_first_table
group by PRODUCT_CATEGORY4, PRODUCT_ID, BRAND_NM
order by total_sum desc
limit 1 by PRODUCT_CATEGORY4

