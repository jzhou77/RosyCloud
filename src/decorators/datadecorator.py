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

# This module defines interface for data decorator

class DataDecorator:
    """This is an interface for data decoration."""
    def __init__(self, DEBUG=False):
        self.DEBUG = DEBUG

    def decorate(self, data):
        """Decorate raw data.
Params:
    data: content to be decorated.

Returns:
    Decorated data.
"""
        raise NotImplementedError("Decorate should be implemented more specific")

    def undecorate(self, data):
        """Undecorate raw data.
Params:
    data: content to be undecorated.

Returns:
    Undecorated data.
"""
        raise NotImplementedError("Undecorate should be implemented more specific")

    def decorate_file(self, ifname, ofname):
        """Decorate data from file to file.
Params:
    ifname: input file path;
    ofname: output file path.

Returns:
    None
"""
        raise NotImplementedError("Decorate file should be implemented more specific")

    def undecorate_file(self, ifname, ofname):
        """Undecorate data from file to file.
Params:
    ifname: input file path;
    ofname: output file path.

Returns:
    None
"""
        raise NotImplementedError("Undecorate file should be implemented more specific")
