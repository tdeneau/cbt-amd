import subprocess
import common
import settings
import monitoring
import os
import sys
import logging


from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger('cbt')

class s3loop:
    def __init__(self, testcfg):
        logger.info ('s3loop cfg = %s' % testcfg)
        self.gw_host = testcfg.get('gw_host', 'localhost')
        self.gw_port = testcfg.get('gw_port', 7480)

    def run(self, id, run_dir):
        outfile = '%s/stress-s3loop-%d.out ' % (run_dir, id)
        p = common.pdsh(settings.getnodes('clients'), 'cd /home/tom/ceph-bringup; bash s3-loop.sh %s:%s /tmp/test %s > %s 2>&1'
                        % (self.gw_host, self.gw_port, id, outfile))
        # logger.info('cd /home/tom/ceph-bringup; bash s3-loop.sh %s:%s /tmp/test %s > %s' % (self.gw_host, self.gw_port, id, outfile))
        return p

class s4loop:
    def __init__(self, testcfg):
        logger.info ('s4loop cfg = %s' % testcfg)

    def run(self, id, run_dir):
        pass

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
        tests = self.config.get('tests')
        logger.info('tests is %s' % (tests))
        for tname in tests.keys():
            testcfg = tests.get(tname)
            tcount = testcfg.get('copies', 0)
            # instantiate test obj
            # first get class based on test name
            module = sys.modules[globals()['__name__']]
            cls = getattr(module, tname, None)
            if (cls is None):
                logger.fatal ('FATAL: no stresstest named %s' % (tname))
                sys.exit()

            testobj = cls(testcfg)
            logger.info ('%s running %s, %d copies' % (testobj, tname, tcount))
            for i in xrange(tcount):
                logger.info ('%s, copy #%d' % (tname, i))
                p = testobj.run(i, self.run_dir)
                if p:
                    ps.append(p)
        for p in ps:
            p.wait()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def recovery_callback(self): 
        pass

    def __str__(self):
        super(StressTest, self).__str__()
