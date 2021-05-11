insert into warehouse.dim_channel(originalchannelid, channelname)
select channel_id, channel_name
from staging.channels
where channel_id not in (select originalchannelid from warehouse.dim_channel);


update warehouse.dim_channel
set channelname = channel_name, loaded_timestamp=now()
from staging.channels c
where originalchannelid = channel_id and channelname <> channel_name
