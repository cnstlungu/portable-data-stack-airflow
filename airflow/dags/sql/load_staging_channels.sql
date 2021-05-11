truncate staging.channels;
with channels as (
select channel_id, channel_name,

row_number() over (partition by channel_id order by loaded_timestamp desc) as rn

from import.channels
)
insert into staging.channels(channel_id, channel_name)
select channel_id, channel_name
from channels
where rn = 1
