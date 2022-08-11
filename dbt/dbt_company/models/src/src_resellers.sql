WITH raw_resellers AS (

    SELECT * FROM {{source('import', 'resellers' )}}
)

SELECT reseller_id, reseller_name, commission_pct , loaded_timestamp

FROM raw_resellers