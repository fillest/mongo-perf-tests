import argparse
import random
import time
import threading
import collections
import util
import pymongo
from pymongo import MongoClient


def insert (collection):
	post = {
		u'action': random.choice((u'ctor', u'play', u'hurrr')),
		u'browser': u'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17',
		u'c_id': random.randint(1, 1000),
		u'ev_id': random.randint(1, 10000),
		# u'v_id': random.randint(1, 1000),
		u'hosted_fqdn': u'unknown_domain',
		u'ip': u'111.112.113.114',
		u'os': u'Windows 7',
		u'parent_fqdn': u'unknown',
		u'referrer_fqdn': u'unknown_domain',
		u'referrer_fqdn1': None,
		u's_id': 0,
		u'time_slice': 0,
		u'timestamp': 1360850805,
		u'user_agent': u'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17',
		u'uuid': u'88c1ed-51f570-210629-abd27b-e95e40-c8165c'
	}
	t = time.time()
	_id = collection.insert(post)
	return time.time() - t

def insert_loop (accum, collection):
	# t_start = time.time()
	# t_last = time.time()
	# try:
		# max_ = 10 * 1000000
		# tick = 50000
		# accum = []
		# for i in range(1, max_ + 1):
		while True:
			latency = insert(collection)

			accum.append(latency)

			# now = time.time()
			# if now - t_last > 1.0:
			# 	# accum.sort()
			# 	# print "insert", "med", median(accum), "p95", p95(accum)
			# 	# del accum[:]
			# 	t_last = now
			
			# if i % tick == 0:
			# 	print "inserted %s of %s" % (i, max_)
	# finally:
	# 	print "took %s s" % 

def read_loop (accum, collection):
	while True:
		t = time.time()
		limit = 50000
		skip = 0
		cur = collection.find({'ev_id': random.randint(1, 10000)}).sort('timestamp').skip(skip).limit(limit)
		# _actually_fetched_data = list(cur)
		for _record in cur:
			d = (60.0 * 2.5) / float(limit)
			time.sleep(d)
		accum.append(time.time() - t)

		time.sleep(1)

def remove_loop (accum, collection):
	while True:
		time.sleep(10)
		print 'removing...'
		t = time.time()
		res = collection.remove({'ev_id': random.randint(1, 10000)})
		print "remove latency", time.time() - t
		assert res['ok'], res
		print "removed num", res['n']

def connect (args):
	client = MongoClient(args.host)
	db = client[args.db_name]
	return db[args.collection_name]

def main ():
	parser = argparse.ArgumentParser()
	parser.add_argument('--host', default = 'mongodb://localhost:27017/')
	parser.add_argument('action', choices = ['insert'])
	parser.add_argument('-it', type = int, default = 2, help = "inserting thread num")
	parser.add_argument('-rt', type = int, default = 1, help = "reading thread num")
	parser.add_argument('-rmt', type = int, default = 1, help = "removing thread num")
	parser.add_argument('-db_name', default = "test_database")
	parser.add_argument('-collection_name', default = "test_collection")
	parser.add_argument('-noi', '--no-index', action = 'store_true', help = "dont create index")
	args = parser.parse_args()

	collection = connect(args)
	print collection.count(), "records in collection %s.%s" % (args.db_name, args.collection_name)

	print "dropping old indexes..."
	try:
		collection.drop_index([("timestamp", pymongo.ASCENDING)])
	except pymongo.errors.OperationFailure as e:
		if "index not found" not in str(e):
			raise 

	if not args.no_index:
		print "trying to create indexes..."
		#pymongo.ASCENDING == 1
		collection.create_index([("ev_id", pymongo.ASCENDING)])
		collection.create_index([("ev_id", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)])
		collection.create_index([("ev_id", pymongo.ASCENDING), ("s_id", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)])
		collection.create_index([("s_id", pymongo.ASCENDING)])

	# insert()
	if args.action == 'insert':
		accum = collections.deque()
		r_accum = collections.deque()
		rm_accum = collections.deque()

		print "starting threads:", args.it, "inserting", args.rt, "reading", args.rmt, "removing"

		for _ in range(args.it):
			collection = connect(args)

			t = threading.Thread(target = insert_loop, args = [accum, collection])
			t.daemon = True
			t.start()

		for _ in range(args.rt):
			collection = connect(args)

			t = threading.Thread(target = read_loop, args = [r_accum, collection])
			t.daemon = True
			t.start()

		for _ in range(args.rmt):
			collection = connect(args)

			t = threading.Thread(target = remove_loop, args = [rm_accum, collection])
			t.daemon = True
			t.start()

		print "starting stat loop..."

		t_start = time.time()
		total_inserts = 0
		total_reads = 0
		try:
			sleepd = 0
			while True:
				time.sleep(1 - sleepd)

				t1 = time.time()


				v = list(accum)
				accum.clear()
				ra = list(r_accum)
				r_accum.clear()

				v.sort()
				ra.sort()
				num = len(v)
				rnum = len(ra)
				total_inserts += num
				total_reads += rnum

				m = util.median(v)
				m = ("%0.5f" % m) if m is not None else None
				p95 = util.p95(v)
				p95 = ("%0.5f" % p95) if p95 is not None else None
				mx = max(v) if v else None
				mx = ("%0.5f" % mx) if mx is not None else None
				print args.it, "ithreads", num, "insert/s, med", m, "p95", p95, "max", mx, "total num", total_inserts

				m = util.median(ra)
				m = (("%0.5f" % m) if m is not None else None) or '-'
				p95 = util.p95(ra)
				p95 = (("%0.5f" % p95) if p95 is not None else None) or '-'
				mx = max(ra) if ra else None
				mx = (("%0.5f" % mx) if mx is not None else None) or '-'
				print " ", args.rt, "rthreads", rnum, "reads/s, med", m, "p95", p95, "max", mx, "total num", total_reads


				sleepd = time.time() - t1
		finally:
			print "total inserts", total_inserts, "total reads", total_reads, "took", (time.time() - t_start), "s"


if __name__ == '__main__':
	main()