{#
  Grain: one row per product. Key: product_id.
  Products are reference data loaded as a dbt seed (not a Kafka topic).
#}
select
    cast(product_id as {{ dbt.type_string() }}) as product_id,
    cast(sku as {{ dbt.type_string() }}) as sku,
    cast(product_name as {{ dbt.type_string() }}) as product_name,
    cast(category as {{ dbt.type_string() }}) as category,
    cast(unit_price as {{ money_type() }}) as unit_price,
    cast(currency as {{ dbt.type_string() }}) as currency,
    cast(is_active as boolean) as is_active
from {{ ref('products') }}
