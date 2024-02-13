SELECT *
FROM {{ ref("prod_master") }}
WHERE product_margin < 0


