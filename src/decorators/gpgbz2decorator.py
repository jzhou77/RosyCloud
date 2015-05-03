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

# This is an implementation of data decorator with bz2 compression followed by gpg encryption
import bz2
import gnupg
import hashlib
import os

import datadecorator

class GPGBZ2Decorator(datadecorator.DataDecorator):
    def __init__(self, gpghome, gpgkeys, compresslevel=5, compressed=True, encrypted=True, DEBUG=False):
        # this function takes home directory for gnupg encryption library
        # and key files
        # if key file does not exist yet, export key to that file
        # return initialized gpg object
        gpg = gnupg.GPG(gnupghome=gpghome)
        try:
            # key exists
            with open(gpgkeys, 'r') as f:
                key = gpg.import_keys(f.read())
        except IOError:
            input_data = gpg.gen_key_input()
            key = gpg.gen_key(input_data)
            ascii_armored_public_keys  = gpg.export_keys(key)
            ascii_armored_private_keys = gpg.export_keys(key, True)
            with open(gpgkeys, 'w') as f:
                f.write(ascii_armored_public_keys)
                f.write(ascii_armored_private_keys)

        self.gpg = gpg
        self.key = key.fingerprints
        self.compresslevel = compresslevel
        self.compressed = compressed
        self.encrypted  = encrypted

    # encrypt with public key
    def decorate(self, data):
        if self.compressed:
            compressed = bz2.compress(data, self.compresslevel)
        else:
            compressed = data

        if self.encrypted:
            encrypted = self.gpg.encrypt(compressed, self.key)
        else:
            encrypted = compressed

        return str(encrypted)

    def undecorate(self, data):
        if self.encrypted:
            decrypted = self.gpg.decrypt(data).data
        else:
            decrypted = data

        if self.compressed:
            decompressed = bz2.decompress(decrypted)
        else:
            decompressed = decrypted

        return decompressed

    def decorate_file(self, ifname, ofname):
        f = file(ifname, 'r')
        buffer_size = 512

        if self.encrypted:
            com_tmp = ofname + '.bz2'
        else:
            com_tmp = ofname

        if self.compressed:
            # use extension to discriminate different files
            compressed_file = bz2.BZ2File(com_tmp, mode='w', \
                buffering = buffer_size, compresslevel=self.compresslevel)
        else:
            compressed_file = open(com_tmp, 'w')

        while True:
            data = f.read(buffer_size)
            if len(data):
                compressed_file.write(data)
            else:
                f.close()
                compressed_file.close()
                break

        if self.encrypted:
            compressed_file = open(com_tmp, 'r')
            self.gpg.encrypt_file(compressed_file, self.key, output=ofname)
            compressed_file.close()

        if not com_tmp == ofname:
            os.unlink(com_tmp)

    def undecorate_file(self, ifname, ofname):
        buffer_size = 512

        output = open(ofname, 'w')
        if self.compressed:
            decrypted_file = ofname + '.bz2'
        else:
            decrypted_file = ifname
            
        if self.encrypted:
            f = open(ifname, 'r')
            self.gpg.decrypt_file(f, output=decrypted_file)
            f.close()

        if self.compressed:
            f = bz2.BZ2File(decrypted_file)
        else:
            f = open(decrypted_file)

        while True:
            data = f.read(buffer_size)
            if len(data):
                output.write(data)
            else:
                f.close()
                output.close()
                break

        if not ofname == decrypted_file:
            os.unlink(decrypted_file)
