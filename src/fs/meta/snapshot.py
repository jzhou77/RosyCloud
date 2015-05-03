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

# This file defines structure for snapshot object
import struct

import dir

class SnapShot:
    # hex-encoded md5 checksum length
    FIELD_LENGTH = 32
    NONE_PARENTS = '0' * FIELD_LENGTH

    # flag constants for extension
    SS_MARKED = 0x1

    # length of field
    SS_LEN_FLAG = 2
    SS_LEN_ROOT = FIELD_LENGTH

    # field offset
    SS_OFS_FLAG = 0
    SS_OFS_ROOT = SS_OFS_FLAG + SS_LEN_FLAG
    SS_OFS_PRNT = SS_OFS_ROOT + SS_LEN_ROOT

    def __init__(self, data=""):
        # empty parent set
        self.flag = 0
        self.parents = []

        if len(data):
            barr = bytearray(data)
            self.flag = struct.unpack('h', \
                str(barr[SnapShot.SS_OFS_FLAG:SnapShot.SS_OFS_ROOT]))[0]
            self.root = dir.DirEntry()
            self.root.mode   = dir.DirEntry.DE_ATTR_DIR
            self.root.fname  = dir.Dir.ROOT_DIR
            self.root.obj_id = \
                str(barr[SnapShot.SS_OFS_ROOT:SnapShot.SS_OFS_PRNT])

            index = SnapShot.SS_OFS_PRNT
            while index < len(barr):
                parent = str(barr[index:index + SnapShot.FIELD_LENGTH])
                if parent == SnapShot.NONE_PARENTS:
                    break
                else:
                    self.parents.append(parent)
                index = index + SnapShot.FIELD_LENGTH
        # create an empty snapshot
        else:
            self.root = None

    # set root directory of the snapshot
    def chroot_dir(self, rdid):
        root_entry = dir.DirEntry()
        root_entry.mode   = dir.DirEntry.DE_ATTR_DIR
        root_entry.fname  = "/"
        root_entry.obj_id = rdid

        self.root = root_entry

    # add new parent to parents set
    def add_parent(self, pid):
        if pid and len(pid):
            self.parents.append(pid)
        else:
            self.parents.append(SnapShot.NONE_PARENTS)

    # set snapshot parents directly
    def set_parents(self, parents):
        self.parents = parents

    # mark snapshot as important
    def mark(self):
        self.flag = self.flag | SnapShot.SS_MARKED

    # check is snapshot marked
    def marked(self):
        return self.flag & SnapShot.SS_MARKED

    def __str__(self):
        barr = bytearray()
        barr = barr + bytearray(struct.pack('h', self.flag))
        barr = barr + bytearray(self.root.obj_id)

        if len(self.parents):
            for parent in self.parents:
               barr = barr + bytearray(parent)
        else:
            barr = barr + bytearray(SnapShot.NONE_PARENTS)

        return str(barr)
