{{
config(
materialized = 'table',
unique_key = 'channel_key'
)
}}


select channel_key, original_channel_id, channel_name
from {{ref('staging_channels')}}
