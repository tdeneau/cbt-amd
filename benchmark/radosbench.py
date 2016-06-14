import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging
import re

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class Radosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Radosbench, self).__init__(cluster, config)

        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.pool_per_proc = config.get('pool_per_proc', False)  # default behavior used to be True
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.object_set_id = config.get('object_set_id', '')

        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', '/usr/bin/rados')
        self.pool = config.get('target_pool', 'rados-bench-cbt')
        self.readmode = config.get('readmode', 'seq')
        self.total_threads = config.get('total_threads', 0)
        using_total_threads = (self.total_threads != 0)
        # if total_threads not explicitly specified, we use product of concurrent_procs and concurrent_ops
        if (not using_total_threads):
            self.total_threads = self.concurrent_procs * self.concurrent_ops
            ops_thr_path = 'concurrent_ops-%08d' % int(self.concurrent_ops)
        else:
            ops_thr_path = 'total_threads-%08d' % int(self.total_threads)

        dir_path = '/concurrent_procs-%08d/osd_ra-%08d/op_size-%08d/%s' % ( int(self.concurrent_procs), int(self.osd_ra), int(self.op_size), ops_thr_path)
        self.run_dir = self.run_dir + dir_path
        self.out_dir = self.archive_dir +  dir_path

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(Radosbench, self).initialize()
        logger.info('Running scrub monitoring.')
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        # set back during development (originally 60)
        pauseSecs = 6
        logger.info('Pausing for %ds for idle monitoring.' % pauseSecs)
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(pauseSecs)
        monitoring.stop()
    
        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        return True

    def run(self):
        super(Radosbench, self).run()
        
        # Remake the pools
        self.mkpools()

        # Run write test
        self._run('write', '%s/write' % self.run_dir, '%s/write' % self.out_dir)
        # Run read test unless write_only
        if self.write_only: return
        self._run(self.readmode, '%s/%s' % (self.run_dir, self.readmode), '%s/%s' % (self.out_dir, self.readmode))
        

    def _run(self, mode, run_dir, out_dir):
        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops
        # determine rados version
        # use clients[0] and assume all clients have same rados version
        client0 = "%s@%s" % (settings.cluster.get('user'), settings.cluster.get('clients')[0])
        # get stdout from first element of tuple
        rados_version_str = common.pdsh(client0, 'rados -v').communicate()[0]
        logger.info("Rados Version: %s" % rados_version_str)
        m = re.findall("version (\d+)", rados_version_str)
        rados_version = int(m[0])

        if mode in ['write'] or rados_version < 9:
            op_size_str = '-b %s' % self.op_size
        else:
            op_size_str = ''


        common.make_remote_dir(run_dir)

        # dump the cluster config
        self.cluster.dump_config(run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(run_dir, recovery_callback)

        # Run rados bench
        monitoring.start(run_dir)
        logger.info('Running radosbench %s test.' % mode)
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            # default behavior is to use a single storage pool 
            pool_name = self.pool
            run_name = '--run-name %s`hostname -s`-%s'%(self.object_set_id, i)
            if self.pool_per_proc: # support previous behavior of 1 storage pool per rados process
                pool_name = 'rados-bench-`hostname -s`-%s'%i
                run_name = ''
            # compute concurrent_ops for this particular process
            base = self.total_threads / self.concurrent_procs
            adj = 1 if (i < (self.total_threads % self.concurrent_procs)) else 0
            ops_this_proc = base + adj 
            # if we compute an ops of 0, no further procs need be invoked
            if ops_this_proc == 0:
                break
            concurrent_ops_str = '--concurrent-ios %s' % ops_this_proc

            if not mode in ['write']:
                setup_cmd = 'echo Maintaining %s concurrent reads of %s bytes for up to %s seconds > %s' % (ops_this_proc, self.op_size, self.time, out_file)
            else:
                setup_cmd = 'rm -f %s' % out_file
        
            rados_bench_cmd = '%s; %s -c %s -p %s bench %s %s %s %s %s --no-cleanup 2> %s >> %s' % \
                 (setup_cmd, self.cmd_path_full, self.tmp_conf, pool_name, op_size_str, self.time, mode, concurrent_ops_str, run_name, objecter_log, out_file)
            p = common.pdsh(settings.getnodes('clients'), rados_bench_cmd)
            ps.append(p)
            time.sleep(0.1)  # added this to avoid server connection problems
        for p in ps:
            p.wait()
        monitoring.stop(run_dir)

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(run_dir)
        common.sync_files('%s/*' % run_dir, out_dir)

    def mkpools(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        if self.pool_per_proc: # allow use of a separate storage pool per process
            for i in xrange(self.concurrent_procs):
                for node in settings.getnodes('clients').split(','):
                    node = node.rpartition("@")[2]
                    self.cluster.rmpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
                    self.cluster.mkpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
        else: # the default behavior is to use a single Ceph storage pool for all rados bench processes
            self.cluster.rmpool('rados-bench-cbt', self.pool_profile)
            self.cluster.mkpool('rados-bench-cbt', self.pool_profile)
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
