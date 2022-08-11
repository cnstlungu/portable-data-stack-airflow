{{ config(
      materialized = 'table'
) }}

SELECT
      id AS cityid,
      cityname,
      countryname,
      regionname
FROM
      {{ ref('geography') }}
