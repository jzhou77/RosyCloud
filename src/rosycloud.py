#!/usr/bin/python 

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

# this is entrance of the cloud disk
import argparse
import hashlib
import os
import sys
import time
import threading

# extended module
import pyinotify
import gnupg

# user defined module
import fs.filesystem
import fs.hddfs
# backup storage
import fs.ossfs
import fs.azurefs
import fs.gdfs
import fs.localfs

import decorators.gpgbz2decorator
import util.util

import fs.meta.dir

import tools.fsck
import tools.ls
import tools.xtr
import tools.tag

import eventhandlers.inotifier

import util.bsddbconn

# system-wise error code
SYS_GLB_CONF_NOT_FOUND = -1       # global configuration file not found
SYS_CLD_CONF_NOT_FOUND = -2       # cloud specific configuration file not found
SYS_RQST_OBJ_NOT_FOUND = -3       # requested object not found

SYS_FILE_EXISTS   = -4            # file already exsists
SYS_UREC_CMD_PARM = -5            # unrecognizable command line parameter
SYS_ASSERT_FAIL   = -6            # cloud storage in a consistent state
SYS_FILE_NOT_EXISTS = -7          # file does not exist

# sync update on snapshot
# root_ss_lock = threading.Lock()
# sync file system hierachy update
fs_hier_lock = threading.Lock()

def myabspath(base, rel):
    return os.path.abspath(base + rel)

# return snapshot id/checksum of lowest common ancestor
def find_lowest_common_ancestor(forked_ss_root, forked_ss_tree):
    # currently, we only support LCA of two nodes
    root1 = forked_ss_root[0]
    root2 = forked_ss_root[1]
    # parents for old snapshot
    parents1   = [] + forked_ss_tree[root1].parents
    curr_level = [] + forked_ss_tree[root1].parents
    next_level = []
    # least common ancestor
    lca = None
    while len(curr_level):
        ss = curr_level.pop()
        next_level = forked_ss_tree[ss].parents + next_level
        parents1   = parents1 + forked_ss_tree[ss].parents
        if len(curr_level) == 0:
            curr_level = next_level
            next_level = []

    parents2 = forked_ss_tree[root2].parents
    while len(parents2):
        # pointer to current parent
        cp = parents2.pop()
        try:
            parents1.index(cp)
            lca = cp
            # got it!
            break
        except ValueError:
            parents2 = forked_ss_tree[cp].parents + parents2

    return lca

# create a list of new dir objects
def three_way_merge(branch1_ss, branch2_ss, base_ss, cloud_fs, local_fs):
    base_hierachy    = fs.filesystem.hierachy(base_ss.root, cloud_fs, local_fs)
    branch1_hierachy = fs.filesystem.hierachy(branch1_ss.root, cloud_fs, local_fs)
    branch2_hierachy = fs.filesystem.hierachy(branch2_ss.root, cloud_fs, local_fs)

    return branch1_hierachy[branch1_ss.root.obj_id].merge(branch1_hierachy, \
            branch2_hierachy[branch2_ss.root.obj_id], branch2_hierachy, \
            base_hierachy[base_ss.root.obj_id], base_hierachy)

# will modify target file system directly
def update(target, repo_fs, new_version, root_ss):
    # pre-order traversal new file system hierachy
    # stack stores object id's
    pre_ord_stk = [root_ss.root.obj_id]

    # path to the file relatively
    bases = ["/"]

    # set up update lists
    while len(pre_ord_stk):
        node_id = pre_ord_stk.pop()
        currnod = new_version[node_id]
        name = currnod[fs.meta.dir.Dir.SELF_REF].fname
        # adjust current path
        base = bases.pop()
        path = os.path.join(base, name)

        sub_dir = [currnod.dir_entries[f].obj_id for f \
                                          in currnod.dir_entries \
                                          if currnod.dir_entries[f].isdir()\
                                          and not f == fs.meta.dir.Dir.SELF_REF]
        # directory has extra content
        # add for pre-order traversal
        pre_ord_stk = pre_ord_stk + sub_dir
        bases = bases + [path] * len(sub_dir)

        # dated storage
        dated = target.find(path, target.fs_hierachy)

        if path == '/' or len(dated) == len(path.split(os.path.sep)):
            # path find, the extra 1 is for root dir
            (created, updated, removed) = currnod.diff(dated.pop())
        else:
            # a new directory
            created = [currnod.dir_entries[f] for f in currnod.dir_entries \
                                 if not f == fs.meta.dir.Dir.SELF_REF]
            # other kinds of changes empty
            updated = []
            removed = []

        ###################################################################
        # sync remote updates to local storage
        ###################################################################
        # create new items
        for e in created:
            relpath = os.path.join(path, e.fname)
            abspath = myabspath(target.configure["SRC_DIR"], relpath)
            if e.isdir():
                target.mkdir(abspath)
            else:
                data = repo_fs.retrieve_to_file(e.obj_id, abspath)
    
        # update modified items
        for e in updated:
            relpath = os.path.join(path, e.fname)
            abspath = myabspath(target.configure["SRC_DIR"], relpath)
            data = repo_fs.retrieve_to_file(e.obj_id, abspath)
    
        # remove obsoleted items
        for e in removed:
            relpath = os.path.join(path, e.fname)
            abspath = myabspath(target.configure["SRC_DIR"], relpath)
            target.remove(abspath)

# synchronize file or directory specified by path
# if interval is 0, do not sync periodically
def sync(remotefs, localfs, interval = 0):
    # The inotify event generated sequentially is ignored.
    localfs.source = False
    # deprecated snapshot tree
    (local_root, local_snapshots) = fs.filesystem.tree_snapshot(localfs)

    local_ss  = localfs.list_snapshots()
    remote_ss = remotefs.list_snapshots()

    # those snapshots have not been cached locally
    diff_ss = list(set(remote_ss) - set(local_ss))
    # cache them
    for ss in diff_ss:
        snapshot = remotefs.get_snapshot(ss)
        localfs.append_snapshot(snapshot, ss)

    # get latest snapshot tree
    (remote_root, remote_snapshots) = fs.filesystem.tree_snapshot(localfs)
    # when first startup, snapshot is empty, keep filesystem untouched
    if len(remote_root):
        if len(remote_root) > 1:
            # merge required
            common_ance = \
                find_lowest_common_ancestor(remote_root, remote_snapshots)
            # no common parent
            if not common_ance:
                empty_root_entry = fs.meta.dir.DirEntry()
                empty_root_entry.mode   = fs.meta.dir.DirEntry.DE_ATTR_DIR
                empty_root_entry.fname  = fs.meta.dir.Dir.ROOT_DIR
                empty_root_entry.obj_id = fs.filesystem.FileSystem.EMPTY_FILE_MD5
                common_ance = fs.meta.snapshot.SnapShot()
                common_ance.root = empty_root_entry
            else:
                # get snapshot object from snapshot id
                common_ance = remote_snapshots[common_ance]
            # currently active snapshot tree
            (new_base_root_dir, new_dir_list) = \
                three_way_merge(remote_snapshots[remote_root[0]], \
                    remote_snapshots[remote_root[1]], common_ance, \
                    remotefs, localfs)
    
            for d in new_dir_list:
                data = str(d)
                obj_id = remotefs.store(data)
                localfs.store_cache(obj_id, data)
    
            snapshot = fs.meta.snapshot.SnapShot()
            snapshot.chroot_dir(new_base_root_dir.obj_id)
            snapshot.add_parent(remote_root[0])
            snapshot.add_parent(remote_root[1])
    
            root_snapshot = remotefs.append_snapshot(snapshot);
            localfs.append_snapshot(snapshot, root_snapshot)
        else:
            root_snapshot = remote_root[0]
            snapshot = remote_snapshots[root_snapshot]
            new_base_root_dir = snapshot.root
        
        # need update pointer to root directory entry
        # also need to diff different hierachies to reflex
        # changes on file system
        new_hier = fs.filesystem.hierachy(new_base_root_dir, remotefs, \
            localfs)
        pre_root_snapshot = localfs.get_root_snapshot_id()
        if pre_root_snapshot:
            localfs.fs_hierachy = fs.filesystem.hierachy( \
                localfs.get_snapshot(pre_root_snapshot).root, \
                remotefs, localfs)

        update(localfs, remotefs, new_hier, snapshot)
        localfs.fs_hierachy   = new_hier
        localfs.set_root_snapshot_id(root_snapshot)

    # End sync-ing
    localfs.source = True

    if interval:
        # re-sync after interval
        threading.Timer(interval, sync, \
            [remotefs, localfs, interval]).start()

# compute md5 given data
def _md5(data):
    md5 = hashlib.md5()
    md5.update(data)

    return md5.hexdigest()

# load configuration file
def load_configure(path):
    configure = {}
    configure_file  = open(path, "r")
    for line in configure_file:
        line = line.rstrip(os.linesep)
        if len(line) > 0 and not line[0] == '#':
            (key, value) = line.split("=", 1)
            configure[key] = os.path.expandvars(value)
    configure_file.close()

    return configure

# load cloud configuration file
def load_cloud_conf(cloud):
    filename  = cloud + ".conf"
    try:
        configure = load_configure(filename)
    except IOError:
        configure = None

    return configure

# load global configuration and initialize if first time startup
def init():
    # read global configuration parameter, program exits if cannot find
    try:
        configure = load_configure('.config')

        # extended parameters
        configure["SYS_DIR_SS"]    = os.path.join(configure["SYS_DIR"], "snapshots")
        configure["SYS_DIR_CACHE"] = os.path.join(configure["SYS_DIR"], "cache")
        configure["SYS_DB"] = os.path.join(configure["SYS_DIR"], "local.db")
        configure["SYS_TMP"] = os.path.join(configure["SYS_DIR"], "tmp")
    except IOError as e:
        print "Cannot file system configuration file. Program exits."
        sys.exit(SYS_GLB_CONF_NOT_FOUND)

    # variable expansion
    configure["SRC_DIR"] = os.path.expandvars( \
            os.path.expanduser(configure["SRC_DIR"]))

    return configure

def init_clouds(clouds, sys_dir):
    # map cloud identifier and their configuration information
    cloud_map = {"oss":("fs.ossfs", "OSSFS"),
                 "azure":("fs.azurefs", "AzureFS"),
                 "googledrive":("fs.gdfs", "GDFS"),
                 "local":("fs.localfs", "LocalFS")}

    decorator = decorators.gpgbz2decorator.GPGBZ2Decorator( \
        configure["GPG_HOME"], configure["GPG_FILE"], DEBUG=DEBUG)

    cloud_fses = []
    for cloud in clouds:
        cloud_conf = load_cloud_conf(cloud)
        cloud_conf["SYS_DIR"] = configure["SYS_DIR"]
        
        cloud_meta = cloud_map[cloud]
        cloud_fses.append(util.util.get_class_by_name( \
            cloud_meta[0], cloud_meta[1])(cloud_conf, decorator))

    return cloud_fses;

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", help="turn on debug information",
        action="store_true")

    # add sub-commands
    subparsers = parser.add_subparsers(help="utility commands")
    # ls: list versioned file
    ls_parser  = subparsers.add_parser("ls", help="list versioned files.")
    ls_parser.add_argument('path', help="path(relative) to query.")
    # xtr: extract versioned file.
    #      if filename specified, extract specific file;
    #      else the whole backup-ed system
    #      if extracted into monitored directory, a new snapshot prod
    xtr_parser = subparsers.add_parser("xtr", help="extract specified file or directory.")
    xtr_parser.add_argument('cloud', help="specify source cloud.")
    xtr_parser.add_argument('version', help="indicate object version number.")
    xtr_parser.add_argument('path', help="recovery from path(relative).")
    # tag: add/delete tags on the backup-ed system.
    tag_parser = subparsers.add_parser("tag", help="tag/de-tag a snapshot.")
    tag_subparser  = tag_parser.add_subparsers(help="actions")
    # add tag sub-sub-parser
    tag_add_parser = tag_subparser.add_parser("add", help="add a new tag on given version of path.")
    tag_add_parser.add_argument("tag", help="name of tag")
    tag_add_parser.add_argument("path", help="add tag on specified path")
    # delete tag sub-sub-parser
    tag_del_parser = tag_subparser.add_parser("delete", help="remove an existing tag.")
    tag_del_parser.add_argument("tag", help="name of tag")
    # fsck: check integrity of the backup-ed file system, and perform
    #       garbage collection.
    fsck_parser = subparsers.add_parser("fsck", help="collect garbage on clouds.")
    fsck_parser.add_argument('-o', '--one', action="store_true", dest="keep_one", help="set keep policy as KEEP_ONE, KEEP_HEUR instead.")
    # start: start sync
    start_parser = subparsers.add_parser("start", help="start synchronizing.")

    args  = parser.parse_args()
    DEBUG = args.debug

    # set up backup environment when first time startup
    configure = init()

    if DEBUG:
        print "init done."

    # distinguish sub-commands
    try:
        if DEBUG:
            action = sys.argv[2]
        else:
            action = sys.argv[1]
    except IndexError:
        action = ""

    if action == "versions":
        db = util.bsddbconn.BSDDBConnector(configure["SYS_DB"])
        clouds = configure["CLOUDS"].split(":")
        cloud_fses = init_clouds(clouds, configure["SYS_DIR"])
        # cloud_fs = fs.cloudemufs.CloudEmulator(configure, DEBUG)
        local_fs = fs.hddfs.HDDFS(configure, db, cloud_fses, DEBUG)
        # list all versioned files
        tools.ls.main(cloud_fses, local_fs, args.path)
    elif action == "xtract":
        # extract versioned file to current working directory
        db     = util.bsddbconn.BSDDBConnector(configure["SYS_DB"])
        clouds = [args.cloud]
        cloud_fses = init_clouds(clouds, configure["SYS_DIR"])
        local_fs   = fs.hddfs.HDDFS(configure, db, cloud_fses, DEBUG)
        ret = tools.xtr.main(args.cloud, local_fs, args.version, args.path)
    elif action == "tag":
        db     = util.bsddbconn.BSDDBConnector(configure["SYS_DB"])
        clouds = configure["CLOUDS"].split(":")
        cloud_fses = init_clouds(clouds, configure["SYS_DIR"])
        local_fs   = fs.hddfs.HDDFS(configure, db, cloud_fses, DEBUG)
        sub_action = sys.argv[3]
        if sub_action == "add":
            op = "-a"
        else:
            op = "-d"
        # tag snapshots
        tools.tag.main(cloud_fses, op, args.tag, args.path)
    elif action == 'fsck':
        db     = util.bsddbconn.BSDDBConnector(configure["SYS_DB"])
        decorator = decorators.gpgbz2decorator.GPGBZ2Decorator( \
            configure["GPG_HOME"], configure["GPG_FILE"], DEBUG=DEBUG)
        cloud_conf = load_cloud_conf('local')
        cloud_conf["SYS_DIR"] = configure["SYS_DIR"]

        cloud_fses = [fs.localfs.LocalFS(cloud_conf, decorator, 'gc_head', 'gc_get', 'gc_put')]
        local_fs = fs.hddfs.HDDFS(configure, db, cloud_fses, DEBUG)
        # collect garbage and check backup integrities
        tools.fsck.main(configure, local_fs, args.keep_one, cloud_fses)
    elif action == "start":
        # normal flow
        omits = [configure["SYS_DIR"] + os.path.sep + \
                 configure["EXCLUDE_FILE"],
                 configure["EXCLUDE_FILE"]]
        try:
            # extract exclude patterns from file, one line a pattern
            exclude_file_path = omits[1]
            exclude_file = open(exclude_file_path, "r")
            for exclude in exclude_file:
                exclude = exclude.rstrip()
                # omit empty lines and comments
                if len(exclude) and not exclude[0] == '#':
                    omits.append(exclude)
                    
            exclude_file.close()
        except IOError as ignored:
            # ignore if exclude file does not exist
            pass

        db = util.bsddbconn.BSDDBConnector(configure["SYS_DB"])
        clouds = configure["CLOUDS"].split(":")
        cloud_fses = init_clouds(clouds, configure["SYS_DIR"])
        local_fs = fs.hddfs.HDDFS(configure, db, omits, cloud_fses, DEBUG)
    
        # the global system maintains a file hierachy
        # we seperate file system hierachy from a specific file system
        # thus, the file system itself is stateless and cost-less to maitain
        snapshot = local_fs.get_root_snapshot_id()
        if snapshot:
            root     = local_fs.get_snapshot(snapshot).root
            local_fs.fs_hierachy = fs.filesystem.hierachy(root, \
                cloud_fses[0], local_fs)
        else:
            empty_dir = fs.meta.dir.empty_dir(fs.meta.dir.Dir.ROOT_DIR)
            local_fs.fs_hierachy = \
                {empty_dir[fs.meta.dir.Dir.SELF_REF].obj_id:empty_dir}

        if DEBUG:
            print "File ignored:", omits
            print "first sync-ing storage"

        # upload all files in root directory
        rootdir, ignore = local_fs.backup_files(configure["SRC_DIR"], "")
        data = str(rootdir)
        local_fs.store_cache(_md5(data), data)
        new_ss = fs.meta.snapshot.SnapShot()
        new_ss.chroot_dir(rootdir)
        new_ss.add_parent(snapshot)
        new_ss_id = local_fs.append_snapshot(new_ss)
        local_fs.set_root_snapshot_id(new_ss_id)

        # sync local and remote storage when startup
        for cloud_fs in cloud_fses:
            # append current state
            cloud_fs.append_snapshot(new_ss)
            # first sync after startup
            sync(cloud_fs, local_fs, int(configure["INTERVAL"]))

        if DEBUG:
            print "first sync done"
    
        # setup inotify-er
        wm   = pyinotify.WatchManager()
    
        # monitor file create event in this simple test case
        mask = pyinotify.IN_CREATE | pyinotify.IN_DELETE | \
               pyinotify.IN_CLOSE_WRITE | \
               pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM | \
               pyinotify.IN_MOVE_SELF
    
        wdd = wm.add_watch(configure["SRC_DIR"], mask, auto_add=True, rec=True)
        notifier = pyinotify.Notifier(wm, \
            eventhandlers.inotifier.NetDiskEventHandler(local_fs, \
                cloud_fs, omits, configure, DEBUG) \
        )
        # notifier.start()
    
        while True:
            print "iterate"
            try:
                notifier.process_events()
                if notifier.check_events():
                    notifier.read_events()
            except KeyboardInterrupt:
                notifier.stop()
                break
