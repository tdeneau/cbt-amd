import subprocess
import common
import settings
import monitoring
import os
import sys
import logging
import re


from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger('cbt')

testTreeDir = '/tmp/test-tree'
populateCmd = '/tmp/cbt/populate.sh'
tmpCbt = '/tmp/cbt'
s3LoopCmd   = 's3-loop.sh'
radosLoopCmd = 'rados-treediff-loop.sh'
rbdLoopCmd = 'rbd-loop.sh'

class stressloop:
    def buildTestTree(self):
        common.pdcp(settings.getnodes('clients'), '', './populate.sh', populateCmd)
        stdout, stderr = common.pdsh(settings.getnodes('clients'), 'bash %s %s' % (populateCmd, testTreeDir)).communicate()
        logger.info ('%s %s' % (stdout, stderr))

    def initialize(self):
        pass

    def run(self, id, run_dir):
        pass

class s3loop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        logger.info ('s3loop cfg = %s' % testcfg)
        self.gw_host = testcfg.get('gw_host', 'localhost')
        self.gw_port = testcfg.get('gw_port', 7480)

    def initialize(self):
        self.buildTestTree()  # need test data for srcdir
        # create the s3user if it does not already exist
        client0 = settings.getnodes('clients')[0:1]
        stdout, stderr = common.pdsh(client0, 'radosgw-admin metadata list user').communicate()
        if not re.compile('"s3user"', re.MULTILINE).findall(stdout):
            common.pdsh(client0, 'radosgw-admin user create --display-name=s3user --uid=s3user --access-key=abc --secret=123')

    def run(self, id, run_dir):
        outfile = '%s/stress-s3loop-%d.out ' % (run_dir, id)
        localS3LoopCmd = '../%s' % s3LoopCmd
        remoteS3LoopCmd = '%s/%s' % (tmpCbt, s3LoopCmd)
        common.pdcp(settings.getnodes('clients'), '', localS3LoopCmd, remoteS3LoopCmd)
        p = common.pdsh(settings.getnodes('clients'), 'bash %s %s:%s %s %s > %s 2>&1'
                        % (remoteS3LoopCmd, self.gw_host, self.gw_port, testTreeDir, id, outfile))
        return p

class radosloop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        logger.info ('radosloop cfg = %s' % testcfg)
        self.pool = testcfg.get('pool', 'rbd')
        self.poolname = "cbt-kernelrbdfio"

    def initialize(self):
        self.buildTestTree()  # need test data for source

    def run(self, id, run_dir):
        outfile = '%s/stress-radosloop-%d.out ' % (run_dir, id)
        localRadosLoopCmd = '../%s' % radosLoopCmd
        remoteRadosLoopCmd = '%s/%s' % (tmpCbt, radosLoopCmd)
        common.pdcp(settings.getnodes('clients'), '', localRadosLoopCmd, remoteRadosLoopCmd)
        p = common.pdsh(settings.getnodes('clients'), 'bash %s %s %s %s > %s 2>&1'
                        % (remoteRadosLoopCmd, self.pool, testTreeDir, id, outfile))
        return p


# dummy for now
class rbdloop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        logger.info ('rbdloop cfg = %s' % testcfg)
        self.poolname = 'cbt-kernelrbdfio'
        self.pool_profile = testcfg.get('pool_profile', 'default')
        self.vol_size = testcfg.get('vol_size', 65536)
        self.stressTestObj = stressTestObj
        self.cluster = stressTestObj.cluster

    def initialize(self):
        self.buildTestTree()  # need test data for source
        # create rbd mapping
        self.mkRbdImages()

    def run(self, id, run_dir):
        outfile = '%s/stress-rbdloop-%d.out ' % (run_dir, id)
        localRbdLoopCmd = '../%s' % rbdLoopCmd
        remoteRbdLoopCmd = '%s/%s' % (tmpCbt, rbdLoopCmd)
        common.pdcp(settings.getnodes('clients'), '', localRbdLoopCmd, remoteRbdLoopCmd)
        p = common.pdsh(settings.getnodes('clients'), 'bash %s %s/%s-`hostname -s` %s %s > %s 2>&1'
                        % (remoteRbdLoopCmd, self.cluster.mnt_dir, self.poolname, testTreeDir, id, outfile))
        return p


    def mkRbdImages(self):
        # first unmount, unmap and rm image if it is already there
        common.pdsh(settings.getnodes('clients'), 'sudo umount /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd -p %s unmap /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd -p %s rm %s-`hostname -s`' % (self.poolname, self.poolname)).communicate()

        # rebuild the pool
        self.cluster.rmpool(self.poolname, self.pool_profile)
        self.cluster.mkpool(self.poolname, self.pool_profile)

        # now create, map and mount the new img
        common.pdsh(settings.getnodes('clients'), 'sudo rbd create %s-`hostname -s` --size %s --pool %s' % (self.poolname, self.vol_size, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd map %s-`hostname -s` --pool %s --id admin' % (self.poolname, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/%s-`hostname -s`' % (self.cluster.mnt_dir, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/rbd/%s/%s-`hostname -s` %s/%s-`hostname -s`' % (self.poolname, self.poolname, self.cluster.mnt_dir, self.poolname)).communicate()
        # print status
        stdout,stderr = common.pdsh(settings.getnodes('clients'), 'rbd showmapped').communicate()
        logger.info ('\n%s %s' % (stdout, stderr))


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

        # clear out the run_dir
        common.pdsh(settings.getnodes('clients'), 'rm -rf %s/*' % self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            logger.info('calling create_recovery_test')
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)
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
            if tcount == 0:
                continue

            # instantiate test obj
            # first get class based on test name
            module = sys.modules[globals()['__name__']]
            cls = getattr(module, tname, None)
            if (cls is None):
                logger.fatal ('FATAL: no stresstest named %s' % (tname))
                sys.exit()
                
            testobj = cls(testcfg, self)
            logger.info ('%s running %s, %d copies' % (testobj, tname, tcount))
            # do any required initialization of this testobj
            testobj.initialize()

            for i in xrange(tcount):
                logger.info ('%s, copy #%d' % (tname, i))
                p = testobj.run(i, self.run_dir)
                if p:
                    ps.append(p)

        # end of for tname in tests.keys():
            
        for p in ps:
            p.wait()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def recovery_callback(self): 
        logger.info('recovery thread called dummy recover_callback')
        pass

    def __str__(self):
        super(StressTest, self).__str__()
