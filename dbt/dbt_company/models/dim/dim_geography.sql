{{ config(
      materialized = 'table',
      unique_key = 'cityid'
) }}

SELECT
      id AS cityid,
      cityname,
      countryname,
      regionname
FROM
      {{ ref('geography') }}
