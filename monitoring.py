import common
import settings

# place to save latest monitoring sar_raw_file based on directory
# passed in to monintoring.start (since we don't have objects here)
latestSarDir = ''
sarRawData = 'sar_raw_data'
sarInterval = 2

def start(directory):
    monDirectory = directory
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
    collectl_dir = '%s/collectl' % directory
    perf_dir = '%s/perf' % directory
    blktrace_dir = '%s/blktrace' % directory
    global latestSarDir
    latestSarDir = sar_dir = '%s/sar' % directory

    # sar
    common.pdsh(nodes, 'mkdir -p -m0755 -- %s' % latestSarDir).communicate()
    common.pdsh(nodes, 'sar -o %s/%s %d' % (latestSarDir, sarRawData, sarInterval))

    # collectl
    # common.pdsh(nodes, 'mkdir -p -m0755 -- %s' % collectl_dir).communicate()
    # common.pdsh(nodes, 'collectl -s+CDJNYZ -o 2cu --utc --plot --rawtoo -i 1:10 -F0 -f %s' % collectl_dir)

    # perf
    # common.pdsh(nodes), 'mkdir -p -m0755 -- %s' % perf_dir).communicate()
    # common.pdsh(nodes), 'cd %s;sudo perf_3.6 record -g -f -a -F 100 -o perf.data' % perf_dir)

    # blktrace
    # common.pdsh(osds, 'mkdir -p -m0755 -- %s' % blktrace_dir).communicate()
    # for device in xrange (0,osds_per_node):
    #     common.pdsh(osds, 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data'
    #                 % (blktrace_dir, device, device))


def stop(directory=None):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')

    common.pdsh(nodes, 'pkill -SIGINT -f collectl').communicate()
    common.pdsh(nodes, 'sudo pkill -SIGINT -f perf_3.6').communicate()
    common.pdsh(settings.getnodes('osds'), 'sudo pkill -SIGINT -f blktrace').communicate()

    # sar output handling
    common.pdsh(nodes, 'pkill -SIGINT -f sar').communicate()
    sarRawInput = '%s/%s' % (latestSarDir, sarRawData)
    common.pdsh(nodes, "sudo gzip %s" % sarRawInput).communicate()

    if directory:
        sc = settings.cluster
        common.pdsh(nodes, 'cd %s/perf;sudo chown %s.%s perf.data' % (directory, sc.get('user'), sc.get('user')))
        make_movies(directory)


def make_movies(directory):
    use_existing = settings.cluster.get('use_existing', True)
    if use_existing:
        return None
    sc = settings.cluster
    seekwatcher = '/home/%s/bin/seekwatcher' % sc.get('user')
    blktrace_dir = '%s/blktrace' % directory

    for device in range(sc.get('osds_per_node')):
        common.pdsh(settings.getnodes('osds'), 'cd %s;%s -t device%s -o device%s.mpg --movie' %
                    (blktrace_dir, seekwatcher, device, device)).communicate()
