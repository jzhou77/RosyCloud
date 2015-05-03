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

# implementation of hard disk drive file system
import os
import sys
import hashlib
import shutil
import fnmatch

import filesystem
import meta.dir

class HDDFS(filesystem.FileSystem):
    # path seperator
    SEPERATOR = os.path.sep
    # root directory
    ROOT = '/'

    # literal constants
    ROOT_SNAPSHOT = "root_snapshot"
    
    # root path
    def __init__(self, configure, db, omits, backup_clouds, DEBUG=False):
        filesystem.FileSystem.__init__(self, DEBUG)

        HDDFS.DEBUG = DEBUG
        if HDDFS.DEBUG:
            print "[DEBUG] Mount netdisk on", configure["SRC_DIR"]
        self.rootpath  = configure["SRC_DIR"]
        if not sys.getfilesystemencoding() == "UTF-8":
            self.rootpath = self.rootpath.encode("UTF-8")
        self.db = db
        # This field indicates direction of inotify events
        # local when true, cloud otherwise
        self.source = True
        self.configure  = configure
        self.bak_clouds = backup_clouds
        self.omits = omits
        # empty cache
        self.snapshots = {}
        self.fs_hierachy = {}

    def list_snapshots(self):
        snapshots = os.listdir(self.configure["SYS_DIR_SS"])
        if HDDFS.DEBUG:
            print "[DEBUG] List snapshots from HDD", snapshots

        return snapshots

    def get_snapshot(self, ss_id):
        if HDDFS.DEBUG:
            print "[DEBUG] HDD get snapshot:", ss_id

        # find in cache
        try:
            snapshot = self.snapshots[ss_id]
        except KeyError:
            ss = open(os.path.join(self.configure["SYS_DIR_SS"], ss_id))
            ss_data  = ss.read()
            # cache it
            snapshot = meta.snapshot.SnapShot(ss_data)
            self.snapshots[ss_id] = snapshot

        return snapshot

    def append_snapshot(self, snapshot, ss_id = ""):
        ss_data = str(snapshot)
        # if no snapshot name specified, digest data
        if not len(ss_id):
            if HDDFS.DEBUG:
                print "[DEBUG] Snapshot id not specifid. Recomputing..."
            m = hashlib.md5()
            m.update(ss_data)
            ss_id = m.hexdigest()

        ss_file = open(os.path.join(self.configure["SYS_DIR_SS"], ss_id), 'wb')
        # string-ize snapshot object
        ss_file.write(ss_data)
        ss_file.close()

        if HDDFS.DEBUG:
            print "[DEBUG] Stored snapshot: ", ss_id

        return ss_id

    def remove_snapshot(self, ss_id):
        obj_id = os.path.join(self.configure["SYS_DIR_SS"], ss_id)
        os.unlink(obj_id)

    # change latest snapshot
    def update_lat_snapshot(self, root, parents_md5):
        snapshot = meta.snapshot.SnapShot()
        snapshot.chroot_dir(root)
        # first update has no parents
        if parents_md5[0]:
            for parent in parents_md5:
                snapshot.add_parent(parent)

        md5 = hashlib.md5()
        md5.update(str(snapshot))
        snapshot_md5 = md5.hexdigest()
        # need negotiate, the granulity of the lock may need coarsening
        self.set_root_snapshot_id(snapshot_md5)
        self.snapshots[snapshot_md5] = snapshot

        return snapshot

    # return a stack consisting of dir objects representing different
    # path component
    # top of the stack is required node
    # path should point to a directory
    def find(self, path, hierachy, root = ""):
        # relative path
        if path == meta.dir.Dir.SELF_REF:
            path = ""

        # stack representing path components
        path_stk = []

        # empty file system
        if not len(root):
            root_snapshot = self.get_root_snapshot_id()
            if root_snapshot:
                root = self.get_snapshot(root_snapshot)
                root = root.root.obj_id
            else:
                path_stk.append( \
                    hierachy[filesystem.FileSystem.EMPTY_FILE_MD5])

                return path_stk

        dir_obj = hierachy[root]
        path_stk.append(dir_obj)

        if path == '/':
            path_com = ['/']
        else:
            # eliminate leading `/'
            path_com = path[1:].split(os.path.sep)
        # skip root directory
        path_idx = 0
        while path_idx < len(path_com):
            try:
                direntry = dir_obj.dir_entries[path_com[path_idx].decode('utf-8').encode('utf-8')]
                dir_obj  = hierachy[direntry.obj_id]
                # assign self ref
                dir_obj.dir_entries[meta.dir.Dir.SELF_REF] = direntry
                path_stk.append(dir_obj)
            except KeyError as e:
                # no such file, add into hierarchy
                path_stk = [hierachy[root]]
                break
            path_idx = path_idx + 1

        return path_stk

    # return dir entry
    def find_entry(self, path, hierachy, root = ""):
        if path[-1] == self.SEPERATOR:
            dir_path = path
            isdir    = True
        else:
            # if not terminated with SEPERATOR, find its parents
            components = path.split(self.SEPERATOR)
            fpath      = components[-1]
            dir_path   = self.SEPERATOR.join(components[:-1])
            isdir      = False

        objs = self.find(dir_path, hierachy, root)
        etys = []
        for obj in objs:
            etys.append(obj.dir_entries[meta.dir.Dir.SELF_REF])
        if not isdir:
            parent = objs.pop()
            try:
                etys.append(parent.dir_entries[fpath])
            except KeyError:
                # if an error occur, return root dir entry
                etys = [etys[0]]

        return etys

    # upload local files onto cloud
    # if path to a directory specified, all files in the directory
    # will be uploaded recursively.
    def backup_files(self, base, path):
        abspath = os.path.join(base, path)
        if os.path.isdir(abspath):
            directory = meta.dir.Dir(path)
            files     = os.listdir(abspath)
            if len(files):
                for f in os.listdir(abspath):
                    if not self.is_omitted(f):
                        entry = meta.dir.DirEntry()
                        mode  = 0
                        if os.path.isdir(os.path.join(abspath, f)):
                            mode = mode | meta.dir.DirEntry.DE_ATTR_DIR
                        entry.mode  = mode
                        entry.fname = f
                        (entry.obj_id, entry.fsize) = \
                            self.backup_files(abspath, f)
                        directory.add_entry(entry)

                # store directory object, all the directory has size 0
                return (self.bak_clouds[0].store(str(directory)), 0)
            else:
                # if empty directory
                # just return required information, no need to create
                # real dir object
                return (filesystem.FileSystem.EMPTY_FILE_MD5, 0)
        else:
            fsize = os.path.getsize(abspath)
            # simply a file
            return (self.bak_clouds[0].store_from_file(abspath), fsize)

    def retrieve(self, path):
        abspath = self._abspath(path)
        if HDDFS.DEBUG:
            print "[DEBUG] Get local file:", abspath

        if self.isdir(path):
            return ""
        else:
            inputfile = file(abspath, "rb")
            data = inputfile.read()
            inputfile.close()
            
            return data
        
    def store(self, path, data):
        abspath = self._abspath(path)
        if HDDFS.DEBUG:
            print "[DEBUG] Store local file:", abspath, "content:", data

        if os.path.isdir(abspath):
            self.mkdir(path)
        else:
            outputfile = file(abspath, "wb")
            outputfile.write(data)

    def store_cache(self, path, data):
        abspath = os.path.join(self.configure["SYS_DIR_CACHE"], path)
        if HDDFS.DEBUG:
            print "[DEBUG] Cache:", abspath

        cache = file(abspath, "wb")
        cache.write(data)

    def retrieve_cache(self, obj_id):
        if HDDFS.DEBUG:
            print "[DEBUG] Retrieve cache:", obj_id
        abspath = os.path.join(self.configure["SYS_DIR_CACHE"], obj_id)

        cache = file(abspath, "rb")
        data  = cache.read()

        if HDDFS.DEBUG:
            print "[DEBUG] Cached data:", len(data)

        return data

    def remove(self, path):
        abspath = self._abspath(path)
        if HDDFS.DEBUG:
            print "[DEBUG] Remove local file:", abspath

        # if file/directory exists, remove the hierachy
        # or all the files has already been removed accompanied
        # with its parental dir
        if os.path.exists(abspath):
            shutil.rmtree(abspath)

    def get_root_snapshot_id(self):
        try:
            return self.db[HDDFS.ROOT_SNAPSHOT]
        except KeyError:
            # no snapshots created yet
            return None

    def set_root_snapshot_id(self, value):
        self.db[HDDFS.ROOT_SNAPSHOT] = value

    # store tag object on cloud
    def tag(self, tag, path):
        """Tag current snapshot to a easy-mem name.
Params:
    tag: tag name;
    path: path to tag."""
        root_snapshot = self.get_root_snapshot_id()
        # we ignore any error when tagging the snapshot
        for cloud in self.clouds:
            cloud.tag_snapshot(tag, root_snapshot, path)

    # remove given tag from cloud
    def detag(self, tag):
        """Given a tag name, remove from cloud.
Params:
    tag: tag id."""
        # also ignore any error
        # just ensure the invariant after processing
        for cloud in self.clouds:
            cloud.remove_tag(tag)

    def move(self, src, dest, isdir=False):
        abssrc  = self._abspath(src)
        absdest = self._abspath(dest)
        if HDDFS.DEBUG:
            print "[DEBUG] Move file from %s to %s." % (abssrc, absdest)
        os.rename(abssrc, absdest)

    def isdir(self, path):
        abspath = self._abspath(path)
        return os.path.isdir(abspath) or abspath[-1] == os.path.sep
        
    def mkdir(self, path):
        abspath = self._abspath(path)
        if HDDFS.DEBUG:
            print "[DEBUG] Make directory:", abspath

        os.mkdir(abspath)

    def native_path(self, global_path):
        rel_path = os.path.relpath(global_path, self.configure["SRC_DIR"])
        if rel_path == meta.dir.Dir.SELF_REF:
            native_path = HDDFS.ROOT
        else:
            native_path = os.path.join(HDDFS.ROOT, rel_path)

        return native_path

    def global_path(self, native_path):
        return os.path.join(self.configure["SRC_DIR"], \
                    os.path.relpath(HDDFS.ROOT, native_path))

    def _travse(self, list, directory, files):
        directory = os.path.relpath(directory, self.rootpath)
        # omit leading dot in relative path
        if directory == ".":
            directory = ""
        if directory in list:
            list.remove(directory)
            list.append(directory + HDDFS.SEPERATOR)
            
        for file in files:
            list.append(os.path.join(directory, file))
            
    def _abspath(self, relpath):
        relpath = relpath.replace(filesystem.FileSystem.COMMON_SEPERATOR,
            HDDFS.SEPERATOR)
        return os.path.join(self.rootpath, relpath)

    def is_omitted(self, filename):
        omitted = False
        for pattern in self.omits:
            if fnmatch.fnmatch(filename, pattern):
                if HDDFS.DEBUG:
                    print "[DEBUG] File", filename, \
                        "match pattern [%s]." % pattern
                omitted = True

        return omitted
