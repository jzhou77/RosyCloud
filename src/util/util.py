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

# This module defines some global utilities
def get_class_by_name(module, name):
    """Load a class by module and name.
Params:
    module: canonical module name, e.g. xx.xxx.xxxx;
    name: class name."""
    modules = module.split('.')
    # load parent module
    m = __import__(module)
    m = getattr(m, modules[-1])

    # get class object
    clazz = getattr(m, name)

    return clazz
