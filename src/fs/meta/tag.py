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

# This file defines structure of tag
# A tag is structured as
# +----------------------+---------------------------------+
# |  snapshot checksum   |      tagged directory path      |
# +----------------------+---------------------------------+
# |        32 bytes      |    256 bytes in case too long   |
# +----------------------+---------------------------------+

class Tag:
    # filed size
    TG_LEN_CHKSM = 32
    TG_LEN_PNAME = 256

    # filed offset
    TG_OFS_CHKSM = 0
    TG_OFS_PNAME = TG_OFS_CHKSM + TG_LEN_CHKSM

    # size of record
    TG_RCSIZE = TG_OFS_PNAME + TG_LEN_PNAME

    def __init__(self, data=""):
        if len(data):
            barr       = bytearray(data)
            self.ss_id = str(barr[Tag.TG_OFS_CHKSM:Tag.TG_OFS_PNAME])
            self.pname = unicode(barr[Tag.TG_OFS_PNAME:]).rstrip('\0\r\n')
        else:
            # no initial data, use empty string as place holder
            self.ss_id = ""
            self.pname = u""

    def __str__(self):
        barr = bytearray()
        barr = barr + bytearray(self.ss_id)
        barr = barr + bytearray(self.pname.encode('utf-8'))
        barr = barr + bytearray('\0' * (Tag.TG_RCSIZE - len(barr)))
