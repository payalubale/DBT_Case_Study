with source_raw as (

    select * from {{ source('stage', 'invoice_raw') }}

),

invoice_raw as (

    select

        customer_nbr as customer_nbr,
        product_nbr as product_id,
        transaction_timestamp as transaction_timestamp,
        region as region,
        zone as zone,
        quantity as quantity

    from source_raw

)

select * from invoice_raw