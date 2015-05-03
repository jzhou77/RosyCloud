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

# This module implement a connector between system and BerkeleyDB
# BerkeleyDB is used to record some extra information used by our
# file system tools offline.

import bsddb

class BSDDBConnector:
    """This class defines interfaces to the BerkeleyDB.
It is used to cache cloud object information for tools
running offline."""
    RECORD_DELIM   = '|'            # record delimiter

    def __init__(self, dbname):
        """Open a hash tables.
Create a new one if not pre-exists.
Params:
    dbname: name of the database file."""
#         self.db = bsddb.hashopen(dbname)
        self.db = dbname

    def put_key(self, key, value):
        """Put append value to given key.
For multiple values, they are seperated by `|' in lexicographic increasing
order.
Params:
    key: key of value to extend.
    value: new value to extend.

Return:
    None."""
        try:
            values = self.db[key].split(BSDDBConnector.RECORD_DELIM)
            values.append(value)
        except KeyError:
            values = [value]

        # keep all value in lexicographic increasing order
        values.sort()
        extended_value = BSDDBConnector.RECORD_DELIM.join(values)
        self.db[key] = extended_value

        self.db.sync()
   

    def remove_key_value(self, key, value):
        """Shrink specific value on given key.
If value shrinks to empty, remove the key.
Params:
    key: key of value to shrink;
    value: value to shrink.

Return:
    None if succeed, otherwise throw ValueError if no such value on given key"""
        try:
            values = self.db[key].split(BSDDBConnector.RECORD_DELIM)
            values.remove(value)
        except ValueError:
            # value not exists, ignore by default
            pass

        if len(values):
            # still some value(s) exist
            shrunk_value = BSDDBConnector.RECORD_DELIM.join(values)
            self.db[key] = shrunk_value
        else:
            # otherwise remove the key
            del self.db[key]

        self.db.sync()

    def __getitem__(self, key):
        """Get value corresponds to given key.
Params:
    key: key value."""
        db   = bsddb.hashopen(self.db)
        value = db[key]
        db.close()

        return value
#         f = open(self.db, 'r')
#         content = f.read()
#         f.close()

        print content.rstrip()

        return content.rstrip()

    def __setitem__(self, key, value):
        """Set value of given key. Database will be sync-ed immediately.
Params:
    key:
    value:"""
        db = bsddb.hashopen(self.db)
        db[key] = value
        db.sync()
        db.close()
#         f = open(self.db, 'w')
#         print >> f, value
#         f.close()
