with prod_mstr_pna1 as (

    select * from {{ ref('prod_mstr_pna1') }}

),
prod_mstr_tpna1 as (

    select * from {{ ref('prod_mstr_tpna1') }}

)

,final as (
    
        select 
            
            p.product_id as product_id,
            pt.product_name as product_name,
            p.product_pricing as product_pricing,
            p.product_margin as product_margin,
            p.prod_date as prod_date,
            case when p.category_code in('CAT-A', 'CAT-B') then 'SNACKS' 
            else 'CEREALS' 
            end as category_code

            from prod_mstr_pna1 as p 
            left join prod_mstr_tpna1 as pt 
            on p.product_id = pt.product_id
            

)
select * from final