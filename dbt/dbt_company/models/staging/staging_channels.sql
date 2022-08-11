with staging_channels as (
select channel_id, channel_name
from {{ref('src_channels')}}
)
select channel_id as channel_key, channel_id as original_channel_id, channel_name
from staging_channels