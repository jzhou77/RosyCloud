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

# this file implements event handler with inotify mechanism
import copy
import fnmatch
import os
import pyinotify
import time

import rosycloud
import fs.meta.dir

class NetDiskEventHandler(pyinotify.ProcessEvent):
    def __init__(self, localfs, remotefs, omit_patterns, conf, DEBUG = False):
        super(pyinotify.ProcessEvent, self).__init__()

        NetDiskEventHandler.DEBUG = DEBUG
        self.localfs = localfs
        self.remotfs = remotefs
        # patterns should be omitted
        self.omits   = omit_patterns
        self.configure = conf
        # distinct different move operation
        self.move_cookie    = 0
        # dir entry object of the moved file/directory
        self.move_src_entry = None
        self.move_from = ""

        self.UPDATE_LOG = open("UPDATE_LOG", "a+")
    
    # create a new dir node
    def process_IN_CREATE(self, event):
        if self.DEBUG:
            print "[DEBUG] Inotify event: IN_CREATE"
            if self.localfs.source:
                print "[DEBUG] Modify filesystem directly."
            else:
                print "[DEBUG] Sync from cloud, filter out."

        if not self._is_file_omitted(event.name) and \
                self.localfs.source:
            if event.dir:
                # create an empty dir reference
                entry = fs.meta.dir.DirEntry()
                entry.mode   = fs.meta.dir.DirEntry.DE_ATTR_DIR
                entry.fname  = event.name
                entry.obj_id = fs.filesystem.FileSystem.EMPTY_FILE_MD5
                entry.fsize  = 0
    
                root, local_snapshots = \
                    fs.filesystem.tree_snapshot(self.localfs)
                assert(len(root) == 1)
                root = root[0]
                self.localfs.fs_hierachy = \
                    fs.filesystem.hierachy( \
                        self.localfs.get_snapshot(root).root, \
                        self.remotfs, self.localfs)
    
                path_stk = self.localfs.find( \
                    self.localfs.native_path(event.path), \
                    self.localfs.fs_hierachy)
                # there must be at least one component, the root entry
                assert(len(path_stk))
                self._update_dir(path_stk, entry)
            else:
                # create a hard link when writing
                tmp_file = self._get_tmp_file_name(event.name)
                os.link(event.pathname, tmp_file)
                    
            # clean up
            self._clear_mv_pair()

    # delete a new dir node
    def process_IN_DELETE(self, event):
        if self.DEBUG:
            print "[DEBUG] Inotify event: IN_DELETE"
            if self.localfs.source:
                print "[DEBUG] Modify filesystem directly."
            else:
                print "[DEBUG] Sync from cloud, filter out."

        if not self._is_file_omitted(event.name) and self.localfs.source:
            root, local_snapshots = \
                fs.filesystem.tree_snapshot(self.localfs)
            assert(len(root) == 1)
            root = root[0]
            self.localfs.fs_hierachy = \
                fs.filesystem.hierachy( \
                    self.localfs.get_snapshot(root).root, \
                    self.remotfs, self.localfs)

            path_stk  = self.localfs.find( \
                self.localfs.native_path(event.path), \
                self.localfs.fs_hierachy)
            old_dir = path_stk.pop()
            dup_dir = copy.deepcopy(old_dir)
            # assert this operation should not fail
            del dup_dir.dir_entries[event.name]

            path_stk.append(dup_dir)
            self._update_dir(path_stk, None)
            tmp_file =  self._get_tmp_file_name(event.name)
            if os.path.exists(tmp_file):
                # file not been linked yet
                os.unlink(tmp_file)

            # clean up
            self._clear_mv_pair()

    # if writed, upload files onto the cloud
    def process_IN_CLOSE_WRITE(self, event):
        if self.DEBUG:
            print "[DEBUG] Inotify event: IN_CLOSE_WRITE"
            if self.localfs.source:
                print "[DEBUG] Modify filesystem directly."
            else:
                print "[DEBUG] Sync from cloud, filter out."

        if not (self._is_file_omitted(event.name) or event.dir) and \
                self.localfs.source:
            tmp_file = self._get_tmp_file_name(event.name)
            if not os.path.exists(tmp_file):
                # file not been linked yet
                os.link(event.pathname, tmp_file)

            root, local_snapshots = \
                fs.filesystem.tree_snapshot(self.localfs)
            assert(len(root) == 1)
            root = root[0]
            self.localfs.fs_hierachy = \
                fs.filesystem.hierachy( \
                    self.localfs.get_snapshot(root).root, \
                    self.remotfs, self.localfs)

            path_stk  = self.localfs.find( \
                self.localfs.native_path(event.path), \
                self.localfs.fs_hierachy)

            md5  = self.remotfs.store_from_file(tmp_file)
            new_entry = fs.meta.dir.DirEntry()
            new_entry.fname  = event.name
            new_entry.obj_id = md5
            # get file size
            new_entry.fsize  = os.stat(tmp_file).st_size

            self._update_dir(path_stk, new_entry)
            # os.unlink(tmp_file)
            # clean up
            self._clear_mv_pair()

    def process_IN_MOVED_FROM(self, event):
        if self.DEBUG:
            print "[DEBUG] Inotify event: IN_MOVED_FROM"
            if self.localfs.source:
                print "[DEBUG] Modify filesystem directly."
            else:
                print "[DEBUG] Sync from cloud, filter out."

        if not self._is_file_omitted(event.name) and self.localfs.source:
            self.move_from = event.name

            root, local_snapshots = \
                fs.filesystem.tree_snapshot(self.localfs)
            assert(len(root) == 1)
            root = root[0]
            self.localfs.fs_hierachy = \
                fs.filesystem.hierachy( \
                    self.localfs.get_snapshot(root).root, \
                    self.remotfs, self.localfs)

            path_stk  = self.localfs.find( \
                self.localfs.native_path(event.path), \
                self.localfs.fs_hierachy)
            old_dir = path_stk.pop()
            # store move information for `move to' to pair
            self.move_cookie    = event.cookie
            self.move_src_entry = old_dir.dir_entries[event.name]
            new_dir = copy.deepcopy(old_dir)
            del new_dir.dir_entries[event.name]
            path_stk.append(new_dir)

            self._update_dir(path_stk, None)

    # only care files moved into the watched directory
    def process_IN_MOVED_TO(self, event):
        if self.DEBUG:
            print "[DEBUG] Inotify event: IN_MOVED_TO"
            if self.localfs.source:
                print "[DEBUG] Modify filesystem directly."
            else:
                print "[DEBUG] Sync from cloud, filter out."

        if not self._is_file_omitted(event.name) and self.localfs.source:
            self.UPDATE_LOG.flush()

            root, local_snapshots = \
                fs.filesystem.tree_snapshot(self.localfs)
            assert(len(root) == 1)
            root = root[0]
            self.localfs.fs_hierachy = \
                fs.filesystem.hierachy( \
                    self.localfs.get_snapshot(root).root, \
                    self.remotfs, self.localfs)

            path_stk  = self.localfs.find( \
                self.localfs.native_path(event.path), \
                self.localfs.fs_hierachy)
            # store data first
            old_dir = path_stk[-1]
            if self.move_cookie == event.cookie:
                # move matched
                entry = self.move_src_entry
                # entry name may be changed
                entry.fname = event.name
                # current snapshot is an intermediate one
                remove_current_ss = True
            else:
                # the same function is for initial sync
                # (md5, size) = self.localfs.backup_files(event.path, event.name)
                (md5, size) = self.localfs.backup_files(tmp_path, event.name)
                entry = fs.meta.dir.DirEntry()
                if event.dir:
                    entry.mode = fs.meta.dir.DirEntry.DE_ATTR_DIR
                entry.fname  = event.name
                entry.obj_id = md5
                entry.fsize  = size
                # current snapshot is a new one
                remove_current_ss = False

            self._update_dir(path_stk, entry, remove_current_ss)
            tmp_from = self._get_tmp_file_name(self.move_from)
            tmp_to   = self._get_tmp_file_name(event.name)
            if os.path.exists(tmp_from):
                # file not been linked yet
                os.rename(tmp_from, tmp_to)
            elif not os.path.exists(tmp_to):
                os.link(event.pathname, tmp_to)
            # clean up
            self._clear_mv_pair()

    def _is_file_omitted(self, path):
        omitted = False
        relpath = os.path.relpath(path, self.localfs.rootpath)
        for pattern in self.omits:
            if fnmatch.fnmatch(relpath, pattern):
                if NetDiskEventHandler.DEBUG:
                    print "[DEBUG] File", relpath, "match pattern [%s]." % pattern
                omitted = True

        return omitted

    # in case event `move from' and `move to' is not sync-ed
    # the rm_current_ss remove the intermediate state
    # if new_entry is None, it means a delete operation performed
    def _update_dir(self, path_stk, new_entry, rm_current_ss=False):
        # use dummy signature for empty file
        # root not created yet
        if not len(path_stk):
            dir_obj = fs.meta.dir.Dir()
            dir_obj.add_entry(new_entry)
            data = str(dir_obj)
            md5  = self.remotfs.store(data)
            self.localfs.store_cache(md5, data)
            # self reference
            self_entry = fs.meta.dir.DirEntry()
            self_entry.mode   = fs.meta.dir.DirEntry.DE_ATTR_DIR
            self_entry.fname  = fs.meta.dir.Dir.ROOT_DIR
            self_entry.obj_id = md5
            dir_obj.dir_entries[fs.meta.dir.Dir.SELF_REF] = self_entry

            rosycloud.fs_hier_lock.acquire()
            self.localfs.fs_hierachy[md5] = dir_obj
            rosycloud.fs_hier_lock.release()
        else:
            if not new_entry:
                new_obj = path_stk.pop()
                data    = str(new_obj)
                # cache inode info
                md5     = self.remotfs.store(data)
                self.localfs.store_cache(md5, data)
                entry   = copy.deepcopy(new_obj.dir_entries[fs.meta.dir.Dir.SELF_REF])
                entry.obj_id = md5

                rosycloud.fs_hier_lock.acquire()
                self.localfs.fs_hierachy[md5] = new_obj
                rosycloud.fs_hier_lock.release()
            else:
                # insertion or modification
                md5   = new_entry.obj_id
                entry = new_entry
            while len(path_stk):
                entry.obj_id = md5
                par_dir = path_stk.pop()
                par_dir = copy.deepcopy(par_dir)
                par_dir.dir_entries[entry.fname] = entry
                data = str(par_dir)
                md5  = self.remotfs.store(data)
                # cache the object locally
                self.localfs.store_cache(md5, data)
                rosycloud.fs_hier_lock.acquire()
                self.localfs.fs_hierachy[md5] = par_dir
                rosycloud.fs_hier_lock.release()
                entry = par_dir[fs.meta.dir.Dir.SELF_REF]

        if rm_current_ss:
            parents_ss = self.localfs.get_snapshot( \
                         self.localfs.get_root_snapshot_id()).parents
            self.remotfs.remove_snapshot(\
                self.localfs.get_root_snapshot_id())
            self.localfs.remove_snapshot(\
                self.localfs.get_root_snapshot_id())
        else:
            parents_ss = [self.localfs.get_root_snapshot_id()]

        # fast forwarding
        # md5 now holds checksum of root directory
        snapshot = self.localfs.update_lat_snapshot(md5, parents_ss)
        ss_data = str(snapshot)

        md5 = self.localfs.append_snapshot(ss_data)
        self.remotfs.append_snapshot(ss_data, md5)

    # clear src information for move
    def _clear_mv_pair(self):
        self.move_cookie  = 0
        self.move_src_md5 = None
        self.move_from    = ""

    # get temporary file path name
    def _get_tmp_file_name(self, filename):
        return os.path.join(self.configure["SYS_TMP"], filename)
