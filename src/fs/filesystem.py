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

# interface of file system
import hashlib
import StringIO

import meta.snapshot

# construct snapshot tree from a list of snapshots
# return checksum of root snapshot and all parsed snapshot
# keyed on checksum
def tree_snapshot(fs):
    if fs.DEBUG:
        print "[DEBUG] Tree-ing snapshot"

    ss_list   = fs.list_snapshots()
    snapshots = {}

    for ss in ss_list:
        snapshots[ss] = fs.get_snapshot(ss)

    # find root snapshot
    for ss_chksum in snapshots:
        ss_list = list(set(ss_list) - set(snapshots[ss_chksum].parents))

    if fs.DEBUG:
        print "[DEBUG] Root snapshot:", ss_list
        print "[DEBUG] Snapshots:", snapshots

    return (ss_list, snapshots)

# construct file system hierachy from a sourcing root dir
# and a backing file system for missing meta
# missing metadata will be cached locally
def hierachy(root_obj_entry, remotefs, localfs):
    # also use a dictionary object to store fs hierachy
    empty_dir = meta.dir.empty_dir(meta.dir.Dir.ROOT_DIR)
    fs_hierachy = {empty_dir[meta.dir.Dir.SELF_REF].obj_id:empty_dir}
    # queue of dir's for later retrieving
    q_obj = [root_obj_entry]
    while len(q_obj):
        dir_entry = q_obj.pop()
        dir_id    = dir_entry.obj_id
        try:
            dir_content = localfs.retrieve(dir_id)
        except IOError as e:
            dir_content = remotefs.retrieve(dir_id)
            localfs.store_cache(dir_id, dir_content)

        # create a new Dir object based on dir content
        folder = meta.dir.Dir(dir_entry, dir_content)
        fs_hierachy[dir_id] = folder

        for obj in folder.dir_entries:
            if not obj == meta.dir.Dir.SELF_REF:
                entry = folder.dir_entries[obj]
                if entry.isdir() and not entry.obj_id in q_obj:
                    q_obj.append(entry)

    return fs_hierachy

# these are interfaces all sub-class should obey
class FileSystem:
    COMMON_SEPERATOR = "/"
    # too large buffer will not introduce noticable imporvement
    BUFFER_SIZE = 4096

    m = hashlib.md5()
    m.update("")
    EMPTY_FILE_MD5 = m.hexdigest()
    
    """Interfaces that all file system should obey"""
    def __init__(self, DEBUG=False):
        # this flag is used when local fs synced from remote fs
        # in this case, all local modifications need not update
        # remote
        self.sync_source = True
        FileSystem.DEBUG = DEBUG
        self.checksums   = {}
        
    def list_snapshots(self):
        """List all the available snapshots on this file system"""
        raise NotImplementedError("List snapshots should be implemented more specific")

    def get_snapshot(self, ss_id):
        """Get specific snapshot"""
        raise NotImplementedError("Get snapshot should be implemented more specific")

    def append_snapshot(self, ss_data, ss_id):
	"""Append newly created snapshot onto the file system"""
        raise NotImplementedError("Append snapshot should be implemented more specific")

    # return checksum for the specifid path if found
    # or an empty string
    def find(self, path):
        """Find specific file on file system"""
        raise NotImplementedError("Find should be implemented more specific")
        
    def get_dir(self, obj_id):
        """Get dir object with specified id"""
        raise NotImplementedError("Get dir should be implemented more specific")

    def get_files(self, seperator = COMMON_SEPERATOR):
        """Files stored on this file system"""
        raise NotImplementedError("Get files should be implemented more specific")

    def retrieve(self, obj_id):
        """Get content of file given its md5 object id"""
        raise NotImplementedError("Retrieve should be implemented more specific")
        
    def store(self, data):
        """
           Store data on cloud, md5 MAC is calculated as object ID
           Return md5 checksum of data if put successfully
        """
        raise NotImplementedError("Store should be implemented more specific")
        
    def remove(self, obj_id):
        """Remove object"""
        raise NotImplementedError("Remove should be implemented more specific")

    def version(self):
        if FileSystem.DEBUG:
            print "[DEBUG] Available files:"
        # checksums of all files
        checksums = {}
        try:
            version_content = StringIO.StringIO(self.retrieve(self.version_file))
            for c in version_content:
                # remove tailing newline '\n'
                c = c.rstrip()
                if len(c):
                    (key, value) = c.split("=")
                    key   = key.decode("UTF-8")
                    value = value.decode("UTF-8")
                    checksums[key] = value
                    if FileSystem.DEBUG:
                        print "[DEBUG]", key, value
        except IOError:
            if FileSystem.DEBUG:
                print "[DEBUG] Create empty version file."
            # if version file does not exist, create an empty one
            self.store(self.version_file, "")

        # set default version number
        checksums[u'.version'] = checksums.get(u'.version', u"0")
        self.checksums = checksums

        return checksums

    def update_version(self, new_version):
        checksums = StringIO.StringIO()
        for key in new_version:
            print >> checksums, u"%s=%s" % (key, new_version[key])

        checksums.flush()
        if FileSystem.DEBUG:
            print "[DEBUG] Update version file."
            print "[DEBUG]     content:", checksums.getvalue()
        self.store(self.version_file, checksums.getvalue().encode("UTF-8"))

    def diff_fs(self, old):
        new_version = self.version()
        old_version = old.version()
        
        # file created
        clist = [f for f in new_version if f not in old_version]
        # file should be updated
        ulist = [f  for f in new_version if f in old_version and \
            not old_version[f] == new_version[f] and not f == '.version']
        # version removed
        rlist = [f for f in old_version if f not in new_version]

        new_checksums = self.checksums
        old_checksums = old.checksums
        for f in clist:
            old_checksums[f] = new_checksums[f]
        for f in ulist:
            old_checksums[f] = new_checksums[f]
        for f in rlist:
            del old_checksums[f]

        clist.sort()
        rlist.sort(reverse=True)
        
        return (clist, ulist, rlist)
        
    def sync_fs(self, src_fs, created, updated, removed):
        # create new files
        for c in created:
            content = src_fs.retrieve(c)
            self.create(c, content)
                
        # updated files
        for u in updated:
            content = src_fs.retrieve(u)
            self.update(u, content)
                
        # remove deprecated files
        for r in removed:
            self.remove(r)
