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

# This is an emulator module for cloud storage
# File will be stored in $HOME/cloud/
import os
import shutil
import hashlib
import StringIO
import datetime

import bakfilesystem
import meta.snapshot
import filesystem

class LocalFS(bakfilesystem.BackupFileSystem):
    ID = "LocalFS"
    SEPERATOR = os.path.sep

    SS_FOLDER = u"ss/"
    TG_FOLDER = u"t/"

    # constructor
    def __init__(self, configure, decorator, HEADLOG="head", GETLOG="get", PUTLOG="put", DEBUG=False):
        # super
        bakfilesystem.BackupFileSystem.__init__(self, DEBUG)

        LocalFS.DEBUG = DEBUG
        self.configure = configure
        self.decorator = decorator

        self.rootpath  = ""
        self.ss_folder = u"ss/"
        self.dir_folder = u"dir/"
        self.tag_folder = u"t/"

        self.storage = self.configure["STORAGE"]

    def get_snapshot_timestamp(self, ss_id):
        path = os.path.join(self.storage, self.ss_folder)
        path = os.path.join(path, ss_id)

        timestamp = os.path.getctime(path)
        ctime = datetime.datetime.fromtimestamp(timestamp)

        return ctime

    def list_snapshots(self):
        """Get all named snapshots"""
        snapshot_path = os.path.join(self.storage, self.ss_folder)

        return os.listdir(snapshot_path)

    def get_snapshot(self, ss_id):
        """Get snapshot content"""
        obj_id = self._join(self.ss_folder, ss_id)
        obj_id = self._join(self.storage, obj_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Get snapshot:", obj_id
        data = file(obj_id).read()
        data = self.decorator.undecorate(data)

        return meta.snapshot.SnapShot(data)

    def append_snapshot(self, ss, ss_id = ""):
        data = str(ss)

        if not len(ss_id):
            md5   = hashlib.md5()
            md5.update(data)
            ss_id = md5.hexdigest()

        obj_id = self._join(self.ss_folder, ss_id)
        obj_id = self._join(self.storage, obj_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Append snapshot:", obj_id

        data = self.decorator.decorate(data)

        f = file(obj_id, 'wb')
        f.write(data)
        f.close()

        return ss_id

    def remove_snapshot(self, ss_id):
        obj_id = self._join(self.ss_folder, ss_id)
        obj_id = self._join(self.storage, obj_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Remove snapshot:", obj_id

        os.remove(obj_id)

    def list_tags(self):
        tag_path = os.path.join(self.storage, self.tag_folder)

        return os.listdir(tag_path)

    def tag_snapshot(self, tag_id, ss_id, path):
        tag = meta.tag.Tag()
        tag.ss_id = ss_id
        tag.pname = path.decode('utf-8').encode('utf-8')
        
        obj_id = self._join(self.tag_folder, tag_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Tag snapshot on path:", obj_id, "@", path
        data = self.decorator.decorate(str(tag))

        f = file(obj_id, 'wb')
        f.write(data)
        f.close()

    def get_tagged_snapshot(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Get tag:", obj_id

        f    = file(obj_id, 'b')
        data = f.read()
        tag  = meta.tag.Tag(self.decorator.decorate(data))
        f.close()

        return tag

    def remove_tag(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Remove tag:", obj_id
        os.remove(obj_id)

    def list_objects(self):
        objects = os.listdir(self.storage)
        objects = [f for f in objects if len(f) == \
            meta.dir.DirEntry.DE_LEN_CHKSM]
        return objects

    def retrieve_to_file(self, obj_id, path):
        if LocalFS.DEBUG:
            print "[DEBUG] Get object:", obj_id

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            file(path, 'w')
        else:
            obj_id = self._join(self.storage, obj_id)
            self.decorator.undecorate_file(obj_id, path)

    def store_from_file(self, path, id=""):
        obj_id = \
            bakfilesystem.BackupFileSystem.store_from_file(self, path, id)
        obj_path = self._join(self.storage, obj_id)
        if LocalFS.DEBUG:
            print "[DEBUG] Store file:", path, "as", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            self.decorator.decorate_file(path, obj_path)

        return obj_id

    def store(self, data, id=""):
        obj_id   = bakfilesystem.BackupFileSystem.store(self, data, id)
        obj_path = self._join(self.dir_folder, obj_id)
        obj_path = self._join(self.storage, obj_path)
        if LocalFS.DEBUG:
            print "[DEBUG] Store object:", obj_id
        data = self.decorator.decorate(data)

        f = file(obj_path, 'wb')
        f.write(data)
        f.close()

        return obj_id

    def retrieve(self, obj_id):
        path = self._join(self.dir_folder, obj_id)
        path = self._join(self.storage, path)
        if LocalFS.DEBUG:
            print "[DEBUG] Get object:", obj_id

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            data = ""
        else:
            f = file(path)
            data = f.read()
            data = self.decorator.undecorate(data)
            f.close()

        return data

    def remove(self, obj_id):
        obj_id = self._join(self.storage, obj_id)

        if LocalFS.DEBUG:
            print "[DEBUG] Remove file:", obj_id

        os.remove(obj_id)

    def _join(self, base, relpath):
        if not base[-1] == LocalFS.SEPERATOR:
            base = base + LocalFS.SEPERATOR

        return base + relpath
