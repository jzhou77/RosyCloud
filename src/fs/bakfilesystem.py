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

# interface of generic backup filesyetem
# although a filesystem interface is too general to abstract the backup
# media.
# Also, we don't constraint the media as cloud only

# system module
import hashlib

# user defined module
import filesystem

class BackupFileSystem(filesystem.FileSystem):
    """Interfaces for backup media"""

    def __init__(self, DEBUG=False):
        BackupFileSystem.DEBUG = DEBUG

    def list_snapshots(self):
        """List all available snapshots on this media.

Return:
    A list of all the snapshots on the backup media
    keyed on id with timestamp as value,
    the timestamp should be added as an extra information for later use.
    Empty list on error currently."""
        raise NotImplementedError("List snapshots should be implemented more specific")

    def get_snapshot(self, ss_id):
        """Get specific snapshot given a snapshot id.
Params:
    ss_id: id of the snapshot to get.

Return:
    A SnapShot object if succeeds. Throws IOError otherwise."""
        raise NotImplementedError("Get snapshot should be implemented more specific")

    def get_snapshot_timestamp(self, ss_id):
        """Get timestamp when snapshot is taken.
Params:
    ss_id: id of the snapshot whose timestamp to get.

Return:
    a datetime object."""
        raise NotImplementedError("Get snapshot timestamp should be implemented more specific")

    def append_snapshot(self, ss, ss_id = ""):
        """Store new snapshot on backup media keyed on md5 checksum.
Params:
    ss: snapshot object to be appended.
    ss_id: checksum of the snapshot. If not specified, will be recomputed

Returns:
    checksum of the snapshot if succeeds. TBD if fails"""
        raise NotImplementedError("Append snapshot should be implemented more specific")

    def remove_snapshot(self, ss_id):
        """Remove specified snapshots.
Params:
    ss_id: checksum of the snapshot to remove.

Returns:
    None. After remove operation, ensure the snapshot does not exist"""
        raise NotImplementedError("Remove snapshot should be implemented more specific")

    def list_tags(self):
        """List all available tags on this media.

Return:
    A list of all the tag names on the backup media.
    Empty list on error currently."""
        raise NotImplementedError("List tags should be implemented more specific")

    def tag_snapshot(self, tag_id, tag_obj):
        """Tag specified snapshot with given name.
Params:
    tag_id: user specified tag name;
    ss_id:  corresponding tagged snapshot id.

Returns:
    None if succeed. Throws IOError otherwise."""
        raise NotImplementedError("Tag snapshot should be implemented more specific")

    def get_tagged_snapshot(self, tag_id):
        """Get snapshot from a given tag.
Params:
    tag_id: user specified tag name.

Returns:
    Corresponding snapshot object if succeed. Throws IOError otherwise."""
        raise NotImplementedError("Get tagged snapshot should be implemented more specific")

    def remove_tag(self, tag_id):
        """Remove specified tag.
Params:
    tag_id: user specified tag name.

Returns:
    Corresponding snapshot object if succeed. Throws IOError otherwise."""
        raise NotImplementedError("Remove tag should be implemented more specific")

    def list_objects(self):
        """List all available objects on this media.

Return:
    A list of all the objects on the backup media
    Empty list on error currently."""
        raise NotImplementedError("List snapshots should be implemented more specific")

    # we want to hide directory structure on backup media
    # thus, we don't provide find interface
    def retrieve_to_file(self, obj_id, path):
        """Get content to a file given its id.
Params:
    obj_id: id of object to get
    path: destination on local filesystem

Return:
    None if succeeds. TBD if fails"""
        raise NotImplementedError("Retrieve to file should be implemented more specific")
        
    def store_from_file(self, path, id = ""):
        """Store specified file on cloud, md5 MAC is calculated as object ID
Params:
    path: absolute path to the local file
    id: if not specifid, md5 of the file will be re-computed and taken as storage key; else use it directly as storage key.

Return:
    md5 checksum of data if put successfully."""
        if not len(id):
            md5 = hashlib.md5()
            f = open(path)
            cont = f.read(filesystem.FileSystem.BUFFER_SIZE)
            while len(cont):
                md5.update(cont)
                cont = f.read(filesystem.FileSystem.BUFFER_SIZE)

            id = md5.hexdigest()

        if BackupFileSystem.DEBUG:
            print "[DEBUG] File ID:", id

        return id

    def store(self, data, id = ""):
        """Store data on cloud, md5 MAC is calculated as object ID
Params:
    path: absolute path to the local file
    id: if not specifid, md5 of the file will be re-computed and taken as storage key; else use it directly as storage key.

Return:
    md5 checksum of data if put successfully."""
        if not len(id):
            # re-compute md5 checksum
            md5 = hashlib.md5()
            md5.update(data)
            id  = md5.hexdigest()

        if BackupFileSystem.DEBUG:
            print "[DEBUG] File ID:", id

        return id

    def retrieve(self, obj_id):
        """Get content to a file given its id.
Params:
    obj_id: id of object to get

Return:
    Object data if succeeds. TBD if fails"""
        raise NotImplementedError("Retrieve should be implemented more specific")
        
    def remove(self, obj_id):
        """Remove object.

Returns:
    None if succeeds. TBD if fails"""
        raise NotImplementedError("Remove should be implemented more specific")

    def get_data_checksum(self, data):
        """Get checksum of data.

Returns:
    checksum of decorated data.
"""
        raise NotImplementedError("Data md5 should be implemented more specific")

    def get_empty_data_md5(self):
        """Get md5 checksum of empty data.

Returns:
    MD5 checksum of empty data after decoration.
"""
        raise NotImplementedError("Empty data md5 should be implemented more specific")
