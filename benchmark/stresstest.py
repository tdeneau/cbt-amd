import subprocess
import common
import settings
import monitoring
import os
import logging


from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger('cbt')

class StressTest(Benchmark):

    def __init__(self, cluster, config):
        super(StressTest, self).__init__(cluster, config)
        dir_path = '/stress-output'
        self.run_dir = self.run_dir + dir_path
        self.out_dir = self.archive_dir +  dir_path
        self.config = config
        logger.info('out_dir is now %s, while run_dir is %s' % (self.out_dir, self.run_dir))

    def initialize(self): 
        super(StressTest, self).initialize()
        return True

    def run(self):
        common.make_remote_dir(self.run_dir)
        logger.info('config is %s' % (self.config))
        ps = []
        tests = ['s3-loop']
        for t in tests:
            tcount = self.config.get(t, 0)
            logger.info ('running %s, %d copies' % (t, tcount))
            for i in xrange(tcount):
                outfile = '%s/stress-%s-%d.out ' % (self.run_dir, t, i)
                logger.info ('%s, copy #%d writing to %s' % (t, i, outfile))
                if (t == 's3-loop'):
                    p = common.pdsh(settings.getnodes('clients'), 'cd /home/tom/ceph-bringup; bash s3-loop.sh Intel-2P-Sandy-Bridge-04:7480 /tmp/test %s > %s' % (i, outfile))
                    ps.append(p)
        for p in ps:
            p.wait()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def recovery_callback(self): 
        pass

    def __str__(self):
        super(StressTest, self).__str__()
