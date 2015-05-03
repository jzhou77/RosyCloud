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

# This file defines interfaces for local file system
# Local file system should have reference to remote/cloud file system
# We can treat local file system a cache of the remote files

class LocalFileSystem:
    def sync(self):
        """Synchronize local file system with remote/cloud file system"""
        pass

    def _tree_snapshot(self, bak_fs = None):
        """Given a backup file system, return the snapshot inheritency.
Params:
    bak_fs: the backup file system whose snapshot inheretency to trace.

Return:
    A tuple (root_snapshot, snapshots) if succeeds. (None, []) otherwise."""
        pass

    def hierachy(self, root_dir_entry):
        """Given the root directory entry, construct the file system hierachy.
Params:
    root_dir_entry: root directory entry object.

Return:
    A list represents the hierachy components. Root is passed as parameter."""

    def list_snapshots(self):
        """List all available snapshots on this media.

Return:
    A list of all the snapshots on the backup media. Empty list on error currently."""
        raise NotImplementedError("List snapshots should be implemented more specific")

    def get_snapshot(self, ss_id):
        """Get specific snapshot given a snapshot id.
Params:
    ss_id: id of the snapshot to get.

Return:
    A SnapShot object if succeeds. TBD if fails"""
        raise NotImplementedError("Get snapshot should be implemented more specific")

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
