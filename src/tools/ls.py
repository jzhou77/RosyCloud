# Copyright (c) 2012,2013 Shuang Qiu <qiush.summer@gmail.com>
#
# This file is part of RosyCloud.
#
# RosyCloud is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# RosyCloud is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with RosyCloud.  If not, see <http://www.gnu.org/licenses/>.

# List historical/versioned file given a file name.
import os

import fs.filesystem
import fs.meta.dir
import fs.meta.snapshot

import util.util

# entrance for list module
def main(clouds, localfs, path):
    """List all versions of a given file on specified clouds.
Params:
    clouds: storage service names on which versioned file is queried;
    filename: name of the file/directory whose versions are wanted."""
    # an array of directory entry storing results
    ver_list = []
    # iterate over all cloud
    for cloud in clouds:
        ver_list = ver_list + list_versions_on_cloud(cloud, localfs, path)

    # sort versioned file on modified time
    ver_list.sort(key=lambda x:x.datetime, reverse=True)
    output_files(path, ver_list)

def list_versions_on_cloud(cloud, local, path):
    """List all version on specified cloud.
Params:
    cloud: cloud on which versions are required.
    filename: required file."""
    # return value
    versions = []

    snapshots = cloud.list_snapshots()
    # tags = cloud.list_tags()
    for ss in snapshots:
        snapshot = cloud.get_snapshot(ss)
        if path == local.ROOT:
            # timestamp
            snapshot.root.datetime = snapshots[ss]
            versions.append(snapshot.root)
        else:
            # currently pass as a parameter
            hierachy = fs.filesystem.hierachy(snapshot.root, cloud, local)
            entry = local.find_entry(path, hierachy, snapshot.root.obj_id)
            # root has been excluded
            if len(entry) > 1:
                entry = entry.pop()
                # entry found
                entry.datetime = cloud.get_snapshot_timestamp(ss)
                # snapshot id by default
                entry.obj_id = ss
                entry.cloud  = cloud.ID
                try:
                    # entry.obj_id   = tags[ss]
                    pass
                except KeyError:
                    # ignore
                    pass
    
                versions.append(entry)

    return versions

def output_files(path, files):
    """Given a list of files, format them out.
Each entry is formatted as:
type(d/f)\tdatetime\tsize\tref id\n
where
type identifies the required file an ordinary file or a directory;
datetime is the creation time of the snapshot;
size of the file
reference id is the tag name or the snapshot id if not tagged.
Params:
    files: list of directory entries to format."""
    # header
    for f in files:
        t = 'd' if f.isdir() else 'f'
        print "%c\t%s\t%s\t%s@%s" % (t, f.datetime, f.fsize, f.obj_id, f.cloud)
