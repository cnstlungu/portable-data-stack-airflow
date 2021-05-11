CHANNEL_DISTRIBUTION = {'direct':(4,2,4), 'reseller': (1,4,5)}

def get_channel_distribution(seller):
    if seller in CHANNEL_DISTRIBUTION.keys():
        distr = CHANNEL_DISTRIBUTION[seller]
        return [*distr[0]*('in-store',), *distr[1]*('web',), *distr[2]*('mobile app',)]

print(get_channel_distribution('direct'))