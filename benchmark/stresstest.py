import subprocess
import common
import settings
import monitoring
import os
import sys
import logging
import re
import threading
import time
import signal
from AsyncFileReader import *

from cluster.ceph import Ceph
from benchmark import Benchmark

import cephfsfio

logger = logging.getLogger('cbt')

# called with list of async readers and length of time to run (0 = forever)
def poll_readers(readers, run_minutes):
    run_secs = run_minutes * 60
    start_time = time.time()
    while len(readers) > 0:
        for rdr in readers:
            if rdr.eof():
                logger.info('% reached EOF' % rdr)
                readers.remove(rdr)
                break
            else:
                while not rdr.queue().empty():
                    line = rdr.queue().get()
                    print line,
        time.sleep(2)
        elapsed_secs = time.time() - start_time
        if run_secs != 0 and elapsed_secs >= run_secs:
            logger.info ('Loop stopped after %d minutes' % (run_minutes))
            break
    # end of while loop            
    return

def getReaderForProc(proc):
    stdout_queue = Queue.Queue()
    stdout_reader = AsyncFileReader(proc.stdout, stdout_queue)
    stdout_reader.start()
    return stdout_reader

class stressloop(object):
    def __init__(self, testcfg, stressTestObj):
        self.stressTestObj = stressTestObj
        self.cluster = stressTestObj.cluster
        self.pool_profile = testcfg.get('pool_profile', 'default')
        self.tmpCbt = '/tmp/cbt'
        self.testTreeDir = '/tmp/test-tree'
        self.fsLoopCmd = 'fs-loop.sh'
        self.populateCmd = 'populate.py'
        logger.info('%s cfg = %s' % (self.__class__.__name__, testcfg))


    def buildTestTree(self):
        remotePopulateCmd = self.makeRemoteCmd('./%s' % self.populateCmd)
        # saw cases where we needed a pause here
        time.sleep(2) 
        stdout, stderr = common.pdsh(settings.getnodes('clients'), 'python %s %s' % (remotePopulateCmd, self.testTreeDir)).communicate()
        logger.info ('\n%s %s' % (stdout, stderr))

    def initialize(self):
        pass

    def run(self, id, run_dir):
        pass

    def rebuildPool(self, poolname = None):
        # rebuild the pool
        if poolname == None:
            poolname = self.poolname
        logger.info ('creating the pool %s' % poolname)
        self.cluster.rmpool(poolname, self.pool_profile)
        self.cluster.mkpool(poolname, self.pool_profile)

    def makeRemoteCmd(self, localCmd):
        # remote just uses the basename in the tmpCbt directory
        remoteCmd = '%s/%s' % (self.tmpCbt, os.path.basename(localCmd))
        common.pdcp(settings.getnodes('clients'), '', localCmd, remoteCmd)
        return remoteCmd

    def pdshClientsShowOutput(self, cmd):
        stdout, stderr = common.pdsh(settings.getnodes('clients'), cmd).communicate()
        print 'stdout=', stdout, '\nstderr=', stderr
        

class s3loop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        super(s3loop, self).__init__(testcfg, stressTestObj)
        self.gw_host = testcfg.get('gw_host', 'localhost')
        self.gw_port = testcfg.get('gw_port', 7480)
        self.s3LoopCmd = 's3-loop.sh'

    def initialize(self):
        self.buildTestTree()  # need test data for srcdir
        # create the s3user if it does not already exist
        head = settings.getnodes('head')
        stdout, stderr = common.pdsh(head, 'radosgw-admin metadata list user').communicate()
        if not re.compile('"s3user"', re.MULTILINE).findall(stdout):
            common.pdsh(head, 'radosgw-admin user create --display-name=s3user --uid=s3user --access-key=abc --secret=123').communicate()
            # saw cases where we needed a pause here
            logger.info('Pausing after user creation')
            time.sleep(20) 
            stdout, stderr = common.pdsh(head, 'radosgw-admin metadata list user').communicate()
            logger.info('radosgw list user output \n%s, \n%s' % (stdout, stderr))


    def run(self, id, run_dir):
        outfile = '%s/stress-s3loop-%d.out ' % (run_dir, id)
        remoteS3LoopCmd = self.makeRemoteCmd('../%s' % self.s3LoopCmd)
        time.sleep(2) 
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            print 'spawn on client ', clientnode
            cmdargs = ['ssh', clientnode, 'bash', remoteS3LoopCmd, '%s:%s' % (self.gw_host, self.gw_port), self.testTreeDir, str(id), '2>&1|tee', outfile]
            p = common.popen(cmdargs)
            pset.append(p)
        return pset

class radosloop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        super(radosloop, self).__init__(testcfg, stressTestObj)
        self.poolname = 'cbt-rados-stress'
        self.threads = testcfg.get('threads', 8)
        self.radosLoopCmd = 'rados-treediff-loop.sh'

    def initialize(self):
        self.buildTestTree()  # need test data for source
        self.rebuildPool()

    def run(self, id, run_dir):
        outfile = '%s/stress-radosloop-%d.out ' % (run_dir, id)
        remoteRadosLoopCmd = self.makeRemoteCmd('../%s' % self.radosLoopCmd)
        # saw cases where we needed a pause here
        time.sleep(2) 
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            print 'spawn on client ', clientnode
            cmdargs = ['ssh', clientnode, 'bash', remoteRadosLoopCmd, self.poolname, self.testTreeDir, str(id), str(self.threads), '2>&1|tee', outfile]
            p = common.popen(cmdargs)
            pset.append(p)
        return pset


class rbdloop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        super(rbdloop, self).__init__(testcfg, stressTestObj)
        self.poolname = 'cbt-rbd-stress'
        self.vol_size = testcfg.get('vol_size', 65536)
        self.rbdLoopCmd = 'rbd-loop.sh'
        self.vol_order = testcfg.get('vol_order', 22)


    def initialize(self):
        self.buildTestTree()  # need test data for source
        # create rbd mapping
        self.mkRbdImages()

    def run(self, id, run_dir):
        outfile = '%s/stress-rbdloop-%d.out ' % (run_dir, id)
        remoteFsLoopCmd = self.makeRemoteCmd('../%s' % self.fsLoopCmd)
        # saw cases where we needed a pause here
        time.sleep(2) 
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            print 'spawn on client ', clientnode
            cmdargs = ['ssh', clientnode, 'bash', remoteFsLoopCmd, '%s/%s-`hostname -s`' % (self.cluster.mnt_dir, self.poolname),
                       self.testTreeDir, str(id), 'rbd', '1', '2>&1|tee', outfile]
            p = common.popen(cmdargs)
            pset.append(p)
        return pset

    def mkRbdImages(self):
        # first unmount, unmap and rm image if it is already there
        logger.info ('unmapping and unmounting rbd images')
        self.pdshClientsShowOutput('sudo umount /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname))
        self.pdshClientsShowOutput('sudo rbd -p %s unmap /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname, self.poolname))
        self.pdshClientsShowOutput('sudo rbd -p %s rm %s-`hostname -s`' % (self.poolname, self.poolname))

        # rebuild the pool
        self.rebuildPool()

        # now create, map and mount the new img
        logger.info ('creating mapping and mounting rbd image')
        # create image using default image-format (2) and default other features
        # (ceph.conf should specify rbd default features = 1)
        # this is required to be able to map with the RBD kernel driver.
        self.pdshClientsShowOutput('sudo rbd create %s-`hostname -s` --size %s --pool %s --order %s' % (self.poolname, self.vol_size, self.poolname, self.vol_order))
        self.pdshClientsShowOutput('sudo rbd map %s-`hostname -s` --pool %s --id admin' % (self.poolname, self.poolname))
        self.pdshClientsShowOutput('sudo mkfs.xfs /dev/rbd/%s/%s-`hostname -s`' % (self.poolname, self.poolname))
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/%s-`hostname -s`' % (self.cluster.mnt_dir, self.poolname)).communicate()
        self.pdshClientsShowOutput('sudo mount -t xfs -o noatime,inode64 /dev/rbd/%s/%s-`hostname -s` %s/%s-`hostname -s`' % (self.poolname, self.poolname, self.cluster.mnt_dir, self.poolname))
        # print status
        stdout,stderr = common.pdsh(settings.getnodes('clients'), 'rbd showmapped').communicate()
        logger.info ('\n%s %s' % (stdout, stderr))
        if not stdout:
            logger.info('rbd showmapped error')
            sys.exit()

#cephfsloop inherits from cephfsfio just to get the fs mkimages_internal stuff
class cephfsloop(stressloop, cephfsfio.CephFsFio):
    def __init__(self, testcfg, stressTestObj):
        stressloop.__init__(self, testcfg, stressTestObj)
        self.monaddr_mountpoint = testcfg.get('monaddr_mountpoint', None)
        self.datapoolname = "cbt-kernelcephfsfiodata"
        self.metadatapoolname = "cbt-kernelcephfsfiometadata"
        self.use_fuse = testcfg.get('use_fuse', False)
        
    def initialize(self):
        self.buildTestTree()  # need test data for source
        # create cephfs mapping
        self.mkimages_internal()

    def run(self, id, run_dir):
        outfile = '%s/stress-cephfsloop-%d.out ' % (run_dir, id)
        remoteFsLoopCmd = self.makeRemoteCmd('../%s' % self.fsLoopCmd)
        # saw cases where we needed a pause here
        time.sleep(2) 
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            print 'spawn on client ', clientnode
            cmdargs = ['ssh', clientnode, 'bash', remoteFsLoopCmd, '%s/cbt-kernelcephfsfio-`hostname -s`' % (self.cluster.mnt_dir), self.testTreeDir, str(id), 'cephfs', '2>&1|tee', outfile]
            p = common.popen(cmdargs)
            pset.append(p)
        return pset


    def __str__(self):
        return "cephfsloop"


#kvmrbdloop
class kvmrbdloop(stressloop):
    def __init__(self, testcfg, stressTestObj):
        stressloop.__init__(self, testcfg, stressTestObj)
        self.vms_per_client = testcfg.get('vms_per_client', 1)
        self.createVmScript = 'create-rbd-vms.py'
        self.loopOnVmsCmd = 'loop-on-vms.sh'
        self.poolname = 'libvirt-pool'
        # following usually set true only for quicker debugging
        self.skipImageSetup = testcfg.get('skip_image_setup', False)
        self.skipVmCreation = testcfg.get('skip_vm_creation', False)

    def initialize(self):

        # first rebuild the pool
        if (self.skipImageSetup or self.skipVmCreation):
            pass
        else:
            self.rebuildPool()

        # to initialize, we need to create the vms on the client machines
        # we do this thru a separate python script
        remoteCreateVmScript = self.makeRemoteCmd('./%s' % self.createVmScript)
        createVmScriptFlags = '-n %d' % self.vms_per_client
        if self.skipImageSetup:
            createVmScriptFlags += ' -I'
        if self.skipVmCreation:
            createVmScriptFlags += ' -V'
            
        # run the vm creation script on each client
        logger.info ('Creating %d VMs on each client, this may take a while' % self.vms_per_client)
        myReaders = []
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            cmdargs = ['ssh', clientnode, 'python', remoteCreateVmScript, createVmScriptFlags, '2>&1']
            p = common.popen(cmdargs)
            pset.append(p)
            myReaders.append(getReaderForProc(p))

        # poll readers and show output
        poll_readers(myReaders, 0)

        # on completion each client should have a file containing the vm ipaddrs
        # and vms are set up for passwordless ssh
        # we need a few more scripts down on the clients (from which they will then get pushed to the vms)
        self.remoteRunOnVmsCmd = self.makeRemoteCmd('./%s' % self.loopOnVmsCmd)
        self.makeRemoteCmd('./%s' % self.populateCmd)
        self.makeRemoteCmd('../%s' % self.fsLoopCmd)
        # note: for this test, we do not need test tree on client, but client will tell vms to build test-tree

    def run(self, id, run_dir):
        outfile = '%s/stress-kvmrbdloop-%d.out ' % (run_dir, id)
        # saw cases where we needed a pause here
        time.sleep(2) 
        pset = []
        for clientnode in self.stressTestObj.cluster.config.get('clients', []):
            logger.info('spawn loop-on-vms.sh on client %s' % clientnode)
            cmdargs = ['ssh', clientnode, 'bash', self.remoteRunOnVmsCmd, '2>&1|tee', outfile]
            proc = common.popen(cmdargs)
            pset.append(proc)
        return pset


    def __str__(self):
        return "kvmrbdloop"



# global ps used by KillSubprocs
ps = []
readers = []
original_sigint = None

def killSubprocs(message):
    for p in ps:
        print message, 'in Ctrl-C handler, killing subprocess ', p.pid
        p.kill()


def exitKillSubprocs(signum, frame):
    killSubprocs('in Ctrl-C handler,')
    sys.exit()


# this is the higher level "benchmark" that is called from the outer cbt framework
# it spawns as many of the stressloop subprocesses as required.
class StressTest(Benchmark):
    def __init__(self, cluster, config):
        super(StressTest, self).__init__(cluster, config)
        dir_path = '/stress-output'
        self.run_dir = self.run_dir + dir_path
        self.out_dir = self.archive_dir +  dir_path
        self.run_minutes = config.get('run_minutes', 0)
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
        global ps, original_sigint
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
                    ps += p
                    print 'ps is now', ps

        # end of for tname in tests.keys():

        # set up SIGINT to kill the subprocesses
        # store the original SIGINT handler
        original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, exitKillSubprocs)

        # create async readers for each subprocess
        for proc in ps:
            readers.append(getReaderForProc(proc))

        # wait for stress tests to finish and meanwhile show stdout
        # poll loop will finish when self.run_minutes elapsed (0 means forever)
        poll_readers(readers, self.run_minutes)

        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        killSubprocs(' ')

    def recovery_callback(self): 
        logger.info('recovery thread called dummy recover_callback')
        pass

    def __str__(self):
        super(StressTest, self).__str__()


