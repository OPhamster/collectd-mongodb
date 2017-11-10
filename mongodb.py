#
# Plugin to collectd statistics from MongoDB
#
import collectd
import numpy as np
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from distutils.version import LooseVersion as V
import datetime as dt

class MongoDB(object):

    def __init__(self):
        self.plugin_name = "mongo"
        self.mongo_host = "127.0.0.1"
        self.mongo_port = 27017
        self.mongo_db = ["admin", ]
        self.mongo_user = None
        self.mongo_password = None

        self.lockTotalTime = None
        self.lockTime = None
        self.accesses = None
        self.misses = None

    def submit(self, type, instance, value, db=None):
        if db:
            plugin_instance = '%s-%s' % (self.mongo_port, db)
        else:
            plugin_instance = str(self.mongo_port)
        v = collectd.Values()
        v.plugin = self.plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()


    def bins_to_histo(self, binned, percentiles):
        vals = {}
        if not binned:
            return vals
        counts = [ d['count'] for d in binned ]
        bins = [ d['micros'] for d in binned]
        cumsum = np.cumsum(counts)

        for ntile in percentiles:
            bin_idx = np.searchsorted(cumsum, np.percentile(cumsum, ntile))
            vals[ntile] = bins[bin_idx]
        return vals


    def do_server_status(self):
        con = MongoClient(host=self.mongo_host, port=self.mongo_port, read_preference=ReadPreference.SECONDARY)
        db = con[self.mongo_db[0]]
        if self.mongo_user and self.mongo_password:
            db.authenticate(self.mongo_user, self.mongo_password)
        server_status = db.command('serverStatus')

        version = server_status['version']
        at_least_2_4 = V(version) >= V('2.4.0')

        # operations
        for k, v in server_status['opcounters'].items():
            self.submit('total_operations', k, v)

        # latencies
        for k,v in server_status['opLatencies'].items():
            self.submit('latency', k, v['latency'])

        # memory
        for t in ['resident', 'virtual', 'mapped']:
            self.submit('memory', t, server_status['mem'][t])

        # connections
        self.submit('connections', 'current', server_status['connections']['current'])
        if 'available' in server_status['connections']:
            self.submit('connections', 'available', server_status['connections']['available'])
        if 'totalCreated' in server_status['connections']:
            self.submit('connections', 'totalCreated', server_status['connections']['totalCreated'])

        # network
        if 'network' in server_status:
            for t in ['bytesIn', 'bytesOut', 'numRequests']:
                      self.submit('bytes', t, server_status['network'][t])

        # locks
        if 'lockTime' in server_status['globalLock']:
            if self.lockTotalTime is not None and self.lockTime is not None:
                if self.lockTime == server_status['globalLock']['lockTime']:
                    value = 0.0
                else:
                    value = float(server_status['globalLock']['lockTime'] - self.lockTime) * 100.0 / float(server_status['globalLock']['totalTime'] - self.lockTotalTime)
                self.submit('percent', 'lock_ratio', value)

            self.lockTime = server_status['globalLock']['lockTime']
        self.lockTotalTime = server_status['globalLock']['totalTime']

        # write lag
        last_write_on_current_host = server_status['repl']['lastWrite']['lastWriteDate']
        time_diff = dt.datetime.now() - last_write_on_current_host
        time_diff_in_seconds = time_diff.total_seconds()
        self.submit('delay', 'writeLag', time_diff_in_seconds)

        # indexes
        if 'indexCounters' in server_status:
            accesses = None
            misses = None
            index_counters = server_status['indexCounters'] if at_least_2_4 else server_status['indexCounters']['btree']

            if self.accesses is not None:
                accesses = index_counters['accesses'] - self.accesses
                if accesses < 0:
                    accesses = None
            misses = (index_counters['misses'] or 0) - (self.misses or 0)
            if misses < 0:
                misses = None
            if accesses and misses is not None:
                self.submit('cache_ratio', 'cache_misses', int(misses * 100 / float(accesses)))
            else:
                self.submit('cache_ratio', 'cache_misses', 0)
            self.accesses = index_counters['accesses']
            self.misses = index_counters['misses']

        for mongo_db in self.mongo_db:
            db = con[mongo_db]
            if self.mongo_user and self.mongo_password:
                con[self.mongo_db[0]].authenticate(self.mongo_user, self.mongo_password)
            db_stats = db.command('dbstats')
            latency_stats=db.matrices.aggregate( [ { "$collStats": { "latencyStats": { "histograms": True } } } ] ).next()

            # stats counts
            self.submit('counter', 'object_count', db_stats['objects'], mongo_db)
            self.submit('counter', 'collections', db_stats['collections'], mongo_db)
            self.submit('counter', 'num_extents', db_stats['numExtents'], mongo_db)
            self.submit('counter', 'indexes', db_stats['indexes'], mongo_db)

            # stats sizes
            self.submit('file_size', 'storage', db_stats['storageSize'], mongo_db)
            self.submit('file_size', 'index', db_stats['indexSize'], mongo_db)
            self.submit('file_size', 'data', db_stats['dataSize'], mongo_db)

            percentiles = {0: '_min', 50: '_p50', 75: '_p75', 90: '_p90', 95: '_p95', 99: '_p99', 100: '_max'}
            for k,v in latency_stats['latencyStats'].items():
                histogram = self.bins_to_histo(v['histogram'], percentiles)
                for ntile, suff in percentiles.items():
                    if ntile in histogram:
                        self.submit('latency', k + suff, histogram[ntile], mongo_db)


            # collection stats
            collections = db.collection_names()
            for collection in collections:
                collection_stats = db.command('collstats', collection)
		if 'size' in collection_stats:
                        self.submit('file_size', (collection + '-' + 'collection_size'), collection_stats['size'], mongo_db)
                if 'count' in collection_stats:
                        self.submit('doc_counter', (collection + '-' + 'total_documents'), collection_stats['count'], mongo_db)
		if 'avgObjSize' in collection_stats:
			avg_doc_size = collection_stats['avgObjSize']
			self.submit('avg_doc_size', (collection + '-' + 'avg_doc_size'), avg_doc_size, mongo_db)
                if 'wiredTiger' in collection_stats:
                    if 'cursor' in collection_stats['wiredTiger']:
                        for k, v in collection_stats['wiredTiger']['cursor'].items():
                            self.submit('collection_stats', (collection + '-' + k), v, mongo_db)

        con.close()


    def config(self, obj):
        for node in obj.children:
            if node.key == 'Port':
                self.mongo_port = int(node.values[0])
            elif node.key == 'Host':
                self.mongo_host = node.values[0]
            elif node.key == 'User':
                self.mongo_user = node.values[0]
            elif node.key == 'Password':
                self.mongo_password = node.values[0]
            elif node.key == 'Database':
                self.mongo_db = node.values
            else:
                collectd.warning("mongodb plugin: Unkown configuration key %s" % node.key)

mongodb = MongoDB()
collectd.register_read(mongodb.do_server_status)
collectd.register_config(mongodb.config)

