------ q0: criando tabela de dados
drop table if exists petlove.analytics.assign_base;
create table petlove.analytics.assign_base (
    id VARCHAR (700),
    created_at VARCHAR (700),
    updated_at VARCHAR (700),
    deleted_at VARCHAR (700),
    name_hash VARCHAR (100),
    email_hash VARCHAR (100),
    address_hash VARCHAR (700),
    birth_date VARCHAR (700),
    status VARCHAR (100),
    version VARCHAR (100),
    city VARCHAR (700),
    state VARCHAR (50),
    neighborhood VARCHAR (700),
    last_date_purchase VARCHAR (700),
    average_ticket NUMERIC(20,15),
    items_quantity INT,
    all_revenue NUMERIC(20,15),
    all_orders INT,
    recency INT,
    marketing_source VARCHAR (700)
);


------ q1: pegando o cumulativo de ativos e churns para estabelecer o churn_rate
with
total as (
    -- segmentando apenas os valores máximos de acumulados de assigns por mês para evitar superestimações
    select distinct date_trunc('month',assign_date)::date as assign_date,
                    max(cum_assign) as cum_assign_base
    from (
          -- fazendo o cumulativo desses valores
          select distinct assign_date::date as assign_date,
                          sum(assign)over(order by assign_date ROWS UNBOUNDED PRECEDING) as cum_assign
          from (
                 -- tratando data e pegando todos os dados referentes a assinantes da base e atribuindo 1
                 select distinct CAST(SPLIT_PART(created_at,' ',1)||' '||SPLIT_PART(created_at,' ',2) AS TIMESTAMP) AS assign_date,
                                 count(id) as assign
                 from petlove.analytics.assign_base
                 group by 1
               )
          group by 1,assign,assign_date
          )
    group by 1
    order by 1
)
,

canceled as (
    -- pegando valores max do cumulativo que representam o total de cancelamentos do mês
    select distinct date_trunc('month',cancel_date)::date as cancel_date,
                    max(cum_cancel) as cum_cancel_base
    from (
          -- fazendo o cumulativo desses valores
          select distinct cancel_date::date as cancel_date,
                          sum(canceled)over(order by cancel_date ROWS UNBOUNDED PRECEDING) as cum_cancel
          from (
                 -- tratando a data e pegando o total de cancelamentos de assinatura por mês
                 select distinct CAST(SPLIT_PART(deleted_at,' ',1)||' '||SPLIT_PART(deleted_at,' ',2) AS TIMESTAMP) AS cancel_date,
                                 count(id) as canceled
                 from petlove.analytics.assign_base
                 where status = 'canceled'
                 group by 1
               )
          group by 1,canceled,cancel_date
          )
    group by 1
    order by 1
)

-- dados final para serem posteriormente tratados
select t.assign_date,
       t.cum_assign_base,
       c.cum_cancel_base,
       c.cum_cancel_base::float / nullif(( t.cum_assign_base -  c.cum_cancel_base),0) as churn_rate,
from total as t
  inner join canceled as c on t.assign_date = c.cancel_date
order by 1;


-- q2: dados para tratamento no Tableau
select *
from petlove.analytics.assign_base

