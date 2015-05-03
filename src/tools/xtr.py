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

# Extract historical/versioned file or snapshots to current directory.
#
# This script is recommended to run offline.
# If run online, all the other storage will be reverted back to
# previous version, which may introduce chaos.

import os

import rosycloud
import fs.filesystem
import fs.meta.dir

import util.util

# entrance for historical version extraction module
def main(cloudname, local, bak_id, filename="/"):
    """Params:
    cloud:  name of cloud, which extract versioned file are specified on;
    bak_id: tag or snapshot number to extract from, snapshot id preferred;
    filename: filename if exists, full path name should be specified."""
    # cloud conf
    conf = rosycloud.load_cloud_conf(cloudname)
    print cloudname
    print "here"
    if not conf:
        # error reading cloud specific configuration file
        return rosycloud.SYS_CLD_CONF_NOT_FOUND

    # the file system driver naming conversion
    # fs.xxxfs
    dv = cloudname + 'fs'
    # module name is lower cased
    # class name is upper cased
    driver = util.util.get_class_by_name('fs.' + dv.lower(), dv.upper())
    # instantiate an driver object with specified configuration
    cloud = driver(conf)

    try:
        snapshot = cloud.get_snapshot(bak_id)
    except IOError:
        try:
            pass
        except IOError:
            # cannot find required snapshot
            return SYS_RQST_OBJ_NOT_FOUND

    # snapshot refer to the required snapshot information
    # hierachy now store all the objects in a hierarchy
    hierachy = fs.filesystem.hierachy(snapshot.root, cloud, local)
    entries  = local.find_entry(filename, hierachy, snapshot.root.obj_id)
    if not filename == local.ROOT and len(entries) == 1:
        return util.error.SYS_FILE_NOT_EXISTS

    paths   = [fs.meta.dir.Dir.SELF_REF]
    dfs_stk = [entries.pop()]
    while len(dfs_stk):
        entry = dfs_stk.pop()
        path  = paths.pop()

        relpath = os.path.join(path, entry.fname)

        if entry.isdir():
            # extract directory, doing recursively
            try:
                # create directory
                os.mkdir(relpath)

                # check recursively
                dir_obj = hierachy[entry.obj_id]
                for de in dir_obj.dir_entries:
                    if not de == fs.meta.dir.Dir.SELF_REF:
                        dfs_stk.append(dir_obj.dir_entries[de])
                        paths.append(relpath)
            except OSError:
                # file exists
                return util.error.SYS_FILE_EXISTS
        else:
            # simple file
            cloud.retrieve_to_file(entry.obj_id, relpath)
