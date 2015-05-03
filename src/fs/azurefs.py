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

# This is the file system module for Azure Blob Storage
import os
import hashlib

import azure
import azure.storage

import filesystem
import bakfilesystem

import meta.snapshot
import meta.dir
import meta.tag

class AzureFS(bakfilesystem.BackupFileSystem):
    ID = "azure"
    BLOB_TYPE = "BlockBlob"
    SEPERATOR = "/"
    TRIALS    = 3

    def __init__(self, configure, decorator, DEBUG = False):
        # super
        bakfilesystem.BackupFileSystem.__init__(self, DEBUG)

        AzureFS.DEBUG  = DEBUG
        self.configure = configure
        self.decorator = decorator
        self.rootpath  = ""
        self.blob_service = azure.storage.BlobService( \
            account_name=self.configure["AZURE_ACCOUNT_NAME"], \
            account_key =self.configure["AZURE_ACCOUNT_KEY"])
        self.ss_folder  = u"ss/"
        self.tag_folder = u"t/"

        # set container name
        try:
            AzureFS.CONTAINER = self.configure["AZURE_CONTAINER"]
        except KeyError:
            print "Azure container is not set properly."
            print "Program exists."

            # container not specified
            sys.exit(-2)

        containers = self.blob_service.list_containers()
        created    = False
        for container in containers.containers:
            # specified container already exists?
            if container.name == AzureFS.CONTAINER:
                created = True
                break

        if not created:
            # container not created yet
            if AzureFS.DEBUG:
                print "[DEBUG] Initialize cloud storage."

            # exit if create container fails after 3 trials
            trials = 1
            succ   = False
            while trials < AzureFS.TRIALS and not succ:
                try:
                    succ = self.blob_service.create_container(AzureFS.CONTAINER)
                except :
                    trials = trials + 1

            if not succ:
                print "Cannot access azure storage. Program exit."
                sys.exit(-1)

    def list_snapshots(self):
        """Get all named snapshots."""
        # the result id time mapping
        ss_dict = {}

        if AzureFS.DEBUG:
            print "[DEBUG] List snapshots."

        results = self.blob_service.list_blobs(AzureFS.CONTAINER, self.ss_folder)
        for blob in results:
            ss_dict[blob.name[len(self.ss_folder):]] = blob.properties.last_modified

        return ss_dict

    def get_snapshot(self, ss_id):
        """Get snapshot content."""
        obj_id = self._join(self.ss_folder, ss_id)
        if AzureFS.DEBUG:
            print "[DEBUG] Get snapshot:", obj_id
        try:
            data = self.retrieve(obj_id)
            snapshot = meta.snapshot.SnapShot(data)
        except azure.WindowsAzureMissingResourceError:
            raise IOError("Snapshot: " + ss_id)

        return snapshot

    def append_snapshot(self, ss, ss_id=""):
        data = str(ss)

        if not len(ss_id):
            md5   = hashlib.md5()
            md5.update(data)
            ss_id = md5.hexdigest()

        obj_id = self._join(self.ss_folder, ss_id)
        if AzureFS.DEBUG:
            print "[DEBUG] Append snapshot:", obj_id
        data = self.decorator.decorate(data)
        # a snapshot cannot be empty, put data directly
        self.blob_service.put_blob(AzureFS.CONTAINER, obj_id, data, \
            x_ms_blob_type=AzureFS.BLOB_TYPE)

        return ss_id

    def remove_snapshot(self, ss_id):
        obj_id = self._join(self.ss_folder, ss_id)
        try:
            self.blob_service.delete_blob(AzureFS.CONTAINER, obj_id)
        except azure.WindowsAzureMissingResourceError:
            # ignore
            pass

    def list_tags(self):
        tags = []

        if AzureFS.DEBUG:
            print "[DEBUG] List tags."

        results = self.blob_service.list_blobs(AzureFS.CONTAINER, self.tag_folder)
        for blob in results:
            tags.append(blob.name[len(self.tag_folder):])

        return tags

    def tag_snapshot(self, tag_obj):
        """Tag specified snapshot and path with given name."""
        if AzureFS.DEBUG:
            print "[DEBUG] New tag:", tag_obj.tag_id, "on path", tag_obj.pname

        data   = self.decorator.decorate(str(tag_obj))
        obj_id = self._join(self.tag_folder, tag_obj.tag_id)
        self.blob_service.put_blob(AzureFS.CONTAINER, obj_id, data, x_ms_blob_type=AzureFS.BLOB_TYPE)

    def get_tagged_snapshot(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if AzureFS.DEBUG:
            print "[DEBUG] Get tag:", obj_id

        try:
            data = self.blob_service.get_blob(AzureFS.CONTAINER, obj_id)
            tag  = meta.tag.Tag(self.decorator.undecorate(data))
        except azure.WindowsAzureMissingResourceError:
            raise IOError("Tag: " + tag_id)

        return tag

    def remove_tag(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if AzureFS.DEBUG:
            print "[DEBUG] Remove tag:", tag_id

        try:
            self.blob_service.delete_blob(AzureFS.CONTAINER, obj_id)
        except azure.WindowsAzureResourceMissingError:
            # ignore, ensure the invariant after the operation
            pass

    # can be implemented with block list for concurrent download
    def retrieve_to_file(self, obj_id, path):
        if AzureFS.DEBUG:
            print "[DEBUG] Get object:", obj_id, "to", path

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # empty file, touch and truncate
            file(path, 'w')
        else:
            tmp_path = self.configure["SYS_DIR"] + "/tmp"
            data = self.blob_service.get_blob(AzureFS.CONTAINER, obj_id)
            f = file(tmp_path, 'wb')
            f.write(data)
            f.close()

            self.decorator.undecorate_file(tmp_path, path)
            # remove temporary file
            os.unlink(tmp_path)

    def store_from_file(self, path, id=""):
        obj_id = \
            bakfilesystem.BackupFileSystem.store_from_file(self, \
                path, id)

        tmp_path = self.configure["SYS_DIR"] + "/tmp"
        self.decorator.decorate_file(path, tmp_path)

        if AzureFS.DEBUG:
            print "[DEBUG] Store file:", path, "as", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            f    = file(tmp_path, 'rb')
            data = f.read()
            self.blob_service.put_blob(AzureFS.CONTAINER, \
                obj_id, data, x_ms_blob_type=AzureFS.BLOB_TYPE)

        # remove temporary file
        os.unlink(tmp_path)

        return obj_id

    def store(self, data, id=""):
        obj_id = bakfilesystem.BackupFileSystem.store(self, data, id)
        if AzureFS.DEBUG:
            print "[DEBUG] Store object:", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            data = self.decorator.decorate(data)
            self.blob_service.put_blob(AzureFS.CONTAINER, \
                obj_id, data, x_ms_blob_type=AzureFS.BLOB_TYPE)

        return obj_id

    def retrieve(self, obj_id):
        if AzureFS.DEBUG:
            print "[DEBUG] Get object:", obj_id

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            data = ""
        else:
            try:
                data = self.blob_service.get_blob(AzureFS.CONTAINER, obj_id)
                data = self.decorator.undecorate(data)
            except azure.WindowsAzureMissingResourceError:
                raise IOError(obj_id)

        return data

    def remove(self, obj_id):
        if AzureFS.DEBUG:
            print "[DEBUG] Remvoe object:", obj_id

        self.blob_service.delete_blob(AzureFS.CONTAINER, obj_id)

    def get_empty_data_md5(self):
        md5 = hashlib.md5()
        md5.update(self.decorator.decorate(""))

        return md5.hexdigest()

    def _join(self, base, rel):
        if not base[-1] == AzureFS.SEPERATOR:
            base = base + AzureFS.SEPERATOR

        return base + rel
