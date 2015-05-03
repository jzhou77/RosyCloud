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

# This module contains all the error codes

# system-wise error code
SYS_GLB_CONF_NOT_FOUND = -1       # global configuration file not found
SYS_CLD_CONF_NOT_FOUND = -2       # cloud specific configuration file not found
SYS_RQST_OBJ_NOT_FOUND = -3       # requested object not found

SYS_FILE_EXISTS   = -4            # file already exsists
SYS_UREC_CMD_PARM = -5            # unrecognizable command line parameter
SYS_ASSERT_FAIL   = -6            # cloud storage in a consistent state
SYS_FILE_NOT_EXISTS = -7          # file does not exist
