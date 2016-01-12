#!/usr/bin/env python
"""When tablesnap fails for whatever reason, you need to manually sync up files
plus create a jsondir file that represents the files you've just added """

import argparse
import boto
from datetime import datetime
import grp
import json
import os
import pwd
import socket
import StringIO

# Needed to load our libraries
from ebs3 import EBS3, BotoServerError

S3_BUCKET = 'update-this'
host = socket.gethostname()
max_file_size = 5120 * 2**20
chunk_size = 256 * 2**20


def get_local_files(args):
    walked = os.walk(args.path)
    datesorted = []
    for dpath, dnames, fnames in walked:
        if 'snapshots' in dpath.split('/'):
            continue
        for f in fnames:
            if not f.endswith('-Data.db') and not f.endswith('-Index.db'):
                # Only check minimun needed for restore since
                # too many S3 API hits will slow us down
                continue
            pth = os.path.join(dpath, f)
            dt = os.path.getctime(pth)
            datesorted.append((dt, pth))
    datesorted.sort()  # most recent last
    return datesorted


def split_sstable(filename):
    '''Generate chunks for multipart upload. '''
    f = open(filename, 'rb')
    while True:
        chunk = f.read(chunk_size)
        if chunk:
            yield StringIO.StringIO(chunk)
        else:
            break
    if f and not f.closed:
        f.close()


def sync_backup(args):
    ''' Checks S3 bucket for files that exist locally. If the file is not
    on S3, and older than age arg, sync to s3 and add a listdir.json file
    '''
    to_sync = []
    try:
        s3conn = EBS3(args.s3cfg, S3_BUCKET)
    except BotoServerError, e:
        print "S3 Response Error [%s]" % e.error_message
        return
    local_files = get_local_files(args)
    for epoch, lfile in local_files:
        # One API call per localfile
        try:
            prefix = "%s:%s" % (host, lfile)
            if args.debug:
                print 'Checking on local file %s' % prefix
            key_list = list(s3conn.get_list(prefix))
        except BotoServerError, e:
            print "S3 Response Error [%s]" % e.error_message
            return

        if len(key_list) == 0:
            # Not backed up yet, check time delta against given args
            now = datetime.now()
            loc_date = datetime.fromtimestamp(epoch)
            delta = now - loc_date
            delta_hours = (delta.seconds / 3600) + (delta.days * 24)
            if delta_hours > args.age:
                if args.debug:
                    print "%s : S3 version old and missing!" % lfile
                to_sync.append(lfile)
    for sync_file in to_sync:
        try:
            stat = os.stat(sync_file)
        except OSError:
            # file compacted away? forget it
            continue
        meta = {'uid': stat.st_uid,
                'gid': stat.st_gid,
                'mode': stat.st_mode}
        try:
            u = pwd.getpwuid(stat.st_uid)
            meta['user'] = u.pw_name
        except:
            pass

        try:
            g = grp.getgrgid(stat.st_gid)
            meta['group'] = g.gr_name
        except:
            pass

        meta = json.dumps(meta)

        # compute md5
        fp = open(sync_file, 'r')
        md5 = boto.utils.compute_md5(fp)
        fp.close()

        key_name = "%s:%s" % (host, sync_file)
        print 'Uploading %s' % key_name
        if args.debug:
            continue
        if stat.st_size > max_file_size:
            print ' ... as multipart'
            mp = s3conn.bucket.initiate_multipart_upload(key_name,
                 metadata={'stat': meta, 'md5sum': md5[0]})
            part = 1
            chunk = None
            try:
                for chunk in split_sstable(sync_file):
                    mp.upload_part_from_file(chunk, part)
                    chunk.close()
                    print ' ... part %d' % part
                    part += 1
                part -= 1
            except Exception as e:
                print 'Error uploading multipart chunk %d' % (part,)
                mp.cancel_upload()
                if chunk:
                    chunk.close()
                raise
            mp.complete_upload()
        else:
            try:
                key = s3conn.bucket.new_key(key_name)
                key.set_metadata('stat', meta)
                key.set_metadata('md5sum', md5[0])
                key.set_contents_from_filename(sync_file, replace=True, md5=md5)
            except:
                print 'ERROR: Failed to upload %s' % key_name
    # Now make sure tablechop doesn't delete it by documenting in a listdir
    dirs_to_list = []
    if to_sync:
        # we'll just list all dirs even if we haven't synced
        dirs_to_list = [os.path.join(args.path, x) \
                        for x in os.listdir(args.path)]
    for ldir in dirs_to_list:
        json_str = json.dumps({ldir: os.listdir(ldir)})
        n = datetime.now().strftime('%Y%m%d%H%M')
        key_name = '%s:%s/tablepunch%s-listdir.json' % (host, ldir, n)
        print 'Uploading %s' % key_name
        if args.debug:
            print json_str
            print
            continue
        key = s3conn.bucket.new_key(key_name)
        key.set_contents_from_string(json_str,
            headers={'Content-Type': 'application/json'},
            replace=True)


def main():
    parser = argparse.ArgumentParser(description='Most recent backup '
                                     'check (for Nagios check)')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Print actions, but don\'t upload anything')
    parser.add_argument('-p', '--path', dest='path', required=True,
                        help='Path to root of SSTables')
    parser.add_argument('-a', '--age', dest='age', required=True, type=int,
                        help='Age of files on disk that need to be synced up. Younger files '
                        'are assumed to be still transferring')
    parser.add_argument('-f', '--s3cfg', dest='s3cfg',
                        help='Path to s3cfg file with credentials.')
    args = parser.parse_args()
    sync_backup(args)

if __name__ == '__main__':
    main()
