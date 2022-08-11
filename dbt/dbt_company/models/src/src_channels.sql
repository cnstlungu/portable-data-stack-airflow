WITH raw_channels AS (

    SELECT * FROM {{source('import', 'channels' )}}
)

select channel_id, channel_name , loaded_timestamp

from raw_channels