select *
    from {{ ref("cust_master") }}
    where cust_nbr is null
