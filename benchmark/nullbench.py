import subprocess
import common
import settings
import monitoring
import os
import logging

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger('cbt')

class Nullbench(Benchmark):

    def __init__(self, cluster, config):
        super(Nullbench, self).__init__(cluster, config)

    def initialize(self): 
        super(Nullbench, self).initialize()

        # clear out the run_dir
        common.pdsh(settings.getnodes('clients'), 'rm -rf %s/*' % self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            logger.info('calling create_recovery_test')
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)
        return True

    def run(self):
        super(Nullbench, self).run()
        
    def recovery_callback(self): 
        pass

    def __str__(self):
        super(Nullbench, self).__str__()
