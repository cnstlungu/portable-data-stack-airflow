{{ config(
      materialized = 'table',
      unique_key = 'geographykey'
) }}

SELECT
      id AS geography_key,
      cityname as city_name,
      countryname as country_name,
      regionname as region_name
FROM
      {{ ref('geography') }}
