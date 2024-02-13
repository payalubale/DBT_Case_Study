{{
    config(
        materialized='incremental',
        unique_key= ['cust_nbr', 'prod_id', 'transaction_date']
    )
}}

with cust as (
    select * from {{ref("cust_master")}}
),
prod as (
    select * from {{ref("prod_master")}}
),
invoice as (
    select * from {{ref("invoice_raw")}}
),
final as (
    select
        date(i.transaction_timestamp) as transaction_date,
        c.cust_nbr,
        c.cust_nm,
        c.cust_loc,
        c.ctry_cd,
        p.product_id as prod_id,
        p.product_name as prod_nm,
        p.category_code as catg_cd,
        i.region,
        i.zone,
        sum(i.quantity) as total_quantity,
        sum(i.quantity*p.product_pricing) as total_value,
        sum(((i.quantity*p.product_pricing)*(p.product_margin))/100) as total_margin,
        count(i.product_id) as total_records
        
 
        from invoice as i
        left join cust as c
        on i.customer_nbr = c.cust_nbr
        left join prod as p
        on i.product_id = p.product_id
        group by c.cust_nbr,c.cust_nm,c.cust_loc,
        c.ctry_cd,p.product_id,p.product_name,p.category_code,i.region,
        i.zone,i.transaction_timestamp
)
 
select * from final

{% if is_incremental() %}

where
  transaction_date > (select max(transaction_date) from {{ this }})

{% endif %}