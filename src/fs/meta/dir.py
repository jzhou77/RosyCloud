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

# this file defines structure of dir
import copy
import hashlib
import io
import struct

import fs.filesystem

# module method
def empty_dir(name):
    empty_dir  = Dir()
    self_entry = DirEntry()
    self_entry.mode   = DirEntry.DE_ATTR_DIR
    self_entry.fname  = name
    self_entry.obj_id = fs.filesystem.FileSystem.EMPTY_FILE_MD5
    empty_dir.dir_entries[Dir.SELF_REF] = self_entry

    return empty_dir


# a directory entry associate storage with file or other dir's
# and is basic building block for a dir record
#
# each entry has 5 attributes
# mode:      indicates file accessing mode
# file name: the real name
# store id:  storage id associated with data
# size:      data size
# source:    where this version copied from
class DirEntry:
    # file attribute constants
    DE_ATTR_DIR  = 0x1

    # field size
    DE_LEN_MODE  = 2
    # file will be zero padded
    DE_LEN_FSIZE = 4
    DE_LEN_FNAME = 128
    DE_LEN_CHKSM = 32

    # field offset
    DE_OFS_MODE  = 0
    DE_OFS_FNAME = DE_OFS_MODE  + DE_LEN_MODE
    DE_OFS_OBJID = DE_OFS_FNAME + DE_LEN_FNAME
    DE_OFS_FSIZE = DE_OFS_OBJID + DE_LEN_CHKSM
    DE_OFS_SOURC = DE_OFS_FSIZE + DE_LEN_FSIZE
    # record size
    DE_RCSIZE    = DE_OFS_SOURC + DE_LEN_CHKSM

    # create an empty directory entry
    def __init__(self, data=""):
        if len(data):
            barr        = bytearray(data)
            self.mode   = struct.unpack('h', str(barr[DirEntry.DE_OFS_MODE:DirEntry.DE_OFS_FNAME]))[0]
            self.fname  = str(barr[DirEntry.DE_OFS_FNAME: \
                DirEntry.DE_OFS_OBJID]).rstrip('\0\r\n').decode("utf-8").encode("utf-8")
            self.obj_id = str(barr[DirEntry.DE_OFS_OBJID: \
                DirEntry.DE_OFS_FSIZE])
            self.fsize  = struct.unpack('i', str(barr[DirEntry.DE_OFS_FSIZE:DirEntry.DE_OFS_SOURC]))[0]
            self.source = str(barr[DirEntry.DE_OFS_SOURC:])
        else:
            self.mode   = 0
            self.fname  = u""
            self.obj_id = ""
            self.fsize  = 0
            self.source = ""

    def isdir(self):
        return self.mode & DirEntry.DE_ATTR_DIR

    def __str__(self):
        barr = bytearray()
        barr = barr + bytearray(struct.pack('h', self.mode))
        barr = barr + bytearray(self.fname.decode('utf-8').encode('utf-8'))
        # padding file name
        barr = barr + bytearray('\0' * (DirEntry.DE_OFS_OBJID - len(barr)))
        barr = barr + bytearray(self.obj_id)
        barr = barr + bytearray(struct.pack('i', self.fsize))
        barr = barr + bytearray(self.source)
        # padding record
        barr = barr + bytearray('\0' * (DirEntry.DE_RCSIZE - len(barr)))

        return str(barr)

class Dir:
    # self reference name
    SELF_REF = '.'
    ROOT_DIR = '/'
    MODIFY_CONF = "modify.conf."
    DELETE_CONF = "delete.conf."


    def __init__(self, dentry = DirEntry(), data=""):
        # empty dir entry 
        # directory object is self-referrable
        # however, if lack information, a dummy dir entry is used
        self.dir_entries = {Dir.SELF_REF:dentry}
        if len(data) % DirEntry.DE_RCSIZE:
            print "[WARNING] unrecognized dir entry dropped," \
                  "data may be corrupted."
            data = data[0:len(data) / DirEntry.DE_RCSIZE * DirEntry.DE_RCSIZE]

        index = 0
        while index < len(data):
            entry = DirEntry(data[index:index + DirEntry.DE_RCSIZE])
            self.dir_entries[entry.fname] = entry
            index = index + DirEntry.DE_RCSIZE

    # if entry already exists, it will be updated
    def add_entry(self, entry):
        self.dir_entries[entry.fname] = entry
        
    # if entry does not exist, an exception will be raised
    def remove_entry(self, fname):
        del self.dir_entries[fname]

    # differ this new dir with an old version
    def diff(self, old_version):
        # newly created files or dirs
        created = [self.dir_entries[f] \
                       for f in self.dir_entries \
                           if f not in old_version.dir_entries]
        # files updated 
        updated = [self.dir_entries[f] \
                       for f in self.dir_entries \
                           if f in old_version.dir_entries and not \
                               self.dir_entries[f].obj_id == \
                               old_version.dir_entries[f].obj_id and not \
                               old_version.dir_entries[f].isdir()]
        # files or dir's removed
        removed = [old_version.dir_entries[f] \
                       for f in old_version.dir_entries \
                           if f not in self.dir_entries]

        return (created, updated, removed)

    # merge directory with a base and remote directory
    # return a new object
    def merge(self, branch1_hier, branch2, branch2_hier, base, base_hier, new_dir_list=[]):
        # new object
        # copy a self reference entry
        self_de = copy.deepcopy(self.dir_entries[Dir.SELF_REF])
        dir_obj = Dir(self_de)
        for entry in self.dir_entries:
            if not entry == Dir.SELF_REF:
                if entry in branch2.dir_entries:
                    # entries in both branches
                    # common item
                    if self.dir_entries[entry].obj_id == \
                            branch2.dir_entries[entry].obj_id:
                        # no conflict
                        dir_obj.add_entry(copy.deepcopy(self.dir_entries[entry]))
                    else:
                        # object id's are not same
                        # a conflict?
                        if entry in base.dir_entries:
                            if base.dir_entries[entry].isdir():
                                # need merge recursively
                                (ignore, new_subdir) = \
                                branch1_hier[entry.obj_id].merge( \
                                branch1_hier, \
                                brach2_hier[branch2.dir_entries[entry].obj_id], \
                                branch2_hier, \
                                base_hier[base.dir_entries[entry].obj_id], \
                                new_dir_list)
    
                                # extend newly created directory objects
                                new_dir_list = new_dir_list + new_subdir
    
                                dir_obj.dir_entries[entry] = \
                                    new_subdir.dir_entries[Dir.SELF_REF].obj_id
                            elif base.dir_entries[entry].obj_id == \
                                    self.dir_entries[entry].obj_id:
                                # branch1 not modified
                                dir_obj.add_entry(copy.deepcopy(branch2.dir_entries[entry]))
                            elif base.dir_entries[entry].obj_id == \
                                    branch2.dir_entries[entry].obj_id:
                                # branch2 not modified
                                dir_obj.add_entry(copy.deepcopy(branch1.dir_entries[entry]))
                            else:
                                # both modify CONFLICT
                                conflicted1 = \
                                    copy.deepcopy(self.dir_entries[entry])
                                conflicted2 = \
                                    copy.deepcopy(branch2.dir_entries[entry])

                                # the md5 larger one prefixed
                                if conflicted1.obj_id < conflicted2.obj_id:
                                    conflicted2.fname = \
                                        Dir.MODIFY_CONF + conflicted2.fname
                                else:
                                    conflicted1.fname = \
                                        Dir.MODIFY_CONF + conflicted1.fname

                                dir_obj.add_entry(conflicted1)
                                dir_obj.add_entry(conflicted2)
                        else:
                            # both newly created
                            # modify CONFLICT
                            conflicted1 = \
                                copy.deepcopy(self.dir_entries[entry])
                            conflicted2 = \
                                copy.deepcopy(branch2.dir_entries[entry])

                            # the md5 larger one prefixed
                            if conflicted1.obj_id < conflicted2.obj_id:
                                conflicted2.fname = \
                                    Dir.MODIFY_CONF + conflicted2.fname
                            else:
                                conflicted1.fname = \
                                    Dir.MODIFY_CONF + conflicted1.fname

                            dir_obj.add_entry(conflicted1)
                            dir_obj.add_entry(conflicted2)
                else:
                    # newly created file? or deleted by branch2?
                    if entry not in base.dir_entries:
                        # maybe newly created
                        dir_obj.add_entry(copy.deepcopy(self.dir_entries[entry]))
                    else:
                        # entry also in base directory
                        # modified?
                        if not self.dir_entries[entry].obj_id == \
                            base.dir_entries[entry].obj_id:
                            # entry modified in this version
                            # while deleted by branch2
                            # then, modify deleted item CONFLICT
                            dir_obj.add_entry(copy.deepcopy(self.dir_entries[entry]))
                            conflicted = \
                                copy.deepcopy(base.dir_entries[entry])
                            conflicted.fname = \
                                Dir.DELETE_CONF + conflicted.fname
                            dir_obj.add_entry(conflicted)
    
                            # else, branch1 does not modify the object
                            # branch2 delete the object
                            # delete the object in merged version

        for entry in branch2.dir_entries:
            if not entry == Dir.SELF_REF:
                if entry not in self.dir_entries and entry not in base.dir_entries:
                    # newly created by branch2
                    dir_obj.add_entry(copy.deepcopy(branch2.dir_entries[entry]))
                elif entry not in self.dir_entries and \
                    entry in base.dir_entries and \
                    not branch2.dir_entries[entry].obj_id == base.dir_entries[entry].obj_id:
                    # delete modify CONFLICT
                        dir_obj.add_entry(copy.deepcopy(branch2.dir_entries[entry]))
                        conflicted = \
                            copy.deepcopy(base.dir_entries[entry])
                        conflicted.fname = \
                            Dir.DELETE_CONF + conflicted.fname
                        dir_obj.add_entry(conflicted)

        m = hashlib.md5()
        m.update(str(dir_obj))
        dir_obj.dir_entries[Dir.SELF_REF].obj_id = m.hexdigest()

        # add newly created directory 
        new_dir_list.append(dir_obj)

        return (dir_obj.dir_entries[Dir.SELF_REF], new_dir_list)

    def __str__(self):
        text = ""
        # we don't record self reference into the storage
        for dir_entry in self.dir_entries:
            if not dir_entry == Dir.SELF_REF:
                text = text + str(self.dir_entries[dir_entry])

        return text

    def __getitem__(self, index):
        return self.dir_entries[index]

    def __delitem__(self, index):
        del self.dir_entries[index]
