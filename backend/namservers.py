from statistics import mean
from backend.accessdb import AccessDB, getnow

class Nameservers:
    def __init__(self, _CONF):
        self.conf = _CONF
        self.timedelta = _CONF['timedelta']
        self.node = _CONF['node']
    
    def resolvetime(self, data, db:AccessDB):
        stats = []
        for ns in data:
            full = []
            short = []
            for time in data[ns]:
                full.append(time[0])
                short.append(time[1])
            stats.append(
                {
                    "node": self.node,
                    "ts": getnow(self.timedelta),
                    "server": ns, 
                    "rtime": mean(full),
                    "rtime_short": mean(short)
                 }
                 )
        db.InsertTimeresolve(stats)