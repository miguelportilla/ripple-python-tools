#!/usr/bin/env python

#    Copyright (c) 2018 Ripple Labs Inc.
#
#    Permission to use, copy, modify, and/or distribute this software for any
#    purpose  with  or without fee is hereby granted, provided that the above
#    copyright notice and this permission notice appear in all copies.
#
#    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
#    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
#    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
#    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
#    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
#    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
#    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# Creates lz4 compressed tar archives from complete
# shards and optinally SCP transfers to a destination

import lz4.frame
import os
import subprocess
import sys
import tarfile

from os import listdir, remove
from os.path import basename, isdir, isfile, join


def validate_args():
    '''Validate command line arguments'''

    usage = ("usage: shard_archiver <shards_directory> <output_directory>"
             " [<indenty_file> <username> <host> <host_directory>]\n"
             "example: shard_archiver.py /db/shards . /.ssh/id_dsa "
             "username domain.com \\\\home\\\\archives\n")

    arg_count = len(sys.argv) - 1
    if arg_count != 2 and arg_count != 6:
        print('Invalid number of arguments.\n')
        print(usage)
        sys.exit(1)

    # Sanity check the shards DB path
    if not isdir(sys.argv[1]):
        print('Invalid shards directory.\n')
        print(usage)
        sys.exit(1)

    # Sanity check the output path
    if not isdir(sys.argv[2]):
        print('Invalid output directory.\n')
        print(usage)
        sys.exit(1)

    if arg_count > 3:
        # Sanity check the identity file
        if not isfile(sys.argv[3]):
            print('Invalid identity file.\n')
            print(usage)
            sys.exit(1)


def read_chunk(file_object):
    while True:
        data = file_object.read(16384)
        if not data:
            break
        yield data


def create_lz4(source_file, output_path):
    src = open(source_file, mode='rb')
    with lz4.frame.open(output_path,
                        mode='wb',
                        block_size=lz4.frame.BLOCKSIZE_MAX1MB,
                        compression_level=lz4.frame.COMPRESSIONLEVEL_MAX,
                        content_checksum=True) as f:
        for piece in read_chunk(src):
            f.write(piece)


def process(args):
    '''Process shard directory'''

    shard_indexes = [d for d in listdir(args[1]) if isdir(
        join(args[1], d)) and d.isdigit()]

    for shard_index in shard_indexes:
        tar_path = join(args[2], shard_index + '.tar')
        lz4_path = join(args[2], shard_index + '.tar.lz4')

        host = None
        dst_path = None
        # If host specified, check if the archive exists on it
        if len(args) > 3:
            host = args[4] + '@' + args[5]
            dst_path = join(args[6], shard_index +
                            '.tar.lz4').replace('\\', '/')
            if subprocess.call(['ssh', '-i', args[3], host,
                                'test -e ' + dst_path]) == 0:
                continue
        # Otherwise check if it exists locally
        elif isfile(lz4_path):
            continue

        shard_dir = join(args[1], shard_index)

        # A NuDB complete shard directory
        # should have a maximum of three files
        if len([name for name in listdir(shard_dir)
                if isfile(join(shard_dir, name))]) > 3:
            continue

        # If a control file is present
        # the shard is not complete
        if isfile(join(shard_dir, 'control.txt')):
            continue

        # Verify the data file is present
        if not isfile(join(shard_dir, 'nudb.dat')):
            continue

        # Verify the key file is present
        if not isfile(join(shard_dir, 'nudb.key')):
            continue

        # Create tar file containing shard directory
        if isfile(tar_path):
            remove(tar_path)
        with tarfile.open(tar_path, "w") as tar:
            tar.add(shard_dir, arcname=basename(shard_dir))

        # Compress the tar file
        create_lz4(tar_path, lz4_path)
        remove(tar_path)

        # If host specified, transfer the archive to it
        if host:
            try:
                subprocess.check_call(['scp', '-i', args[3], lz4_path,
                                      "%s:%s" % (host, dst_path)])
                remove(lz4_path)
            except:
                print('SCP to host failed')


if __name__ == "__main__":
    validate_args()
    process(sys.argv)
