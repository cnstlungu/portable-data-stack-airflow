{{
config(
materialized = 'table'
)
}}


select channel_key, original_channel_id, channel_name
from {{ref('staging_channels')}}
