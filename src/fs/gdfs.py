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

# This is the file system module for Google Drive cloud service
import io
import sys
import os
import hashlib

import httplib2

# extended modules
import apiclient.discovery
import apiclient.errors
import oauth2client.file
import oauth2client.client
import oauth2client.tools

# user defined modules
import filesystem
import bakfilesystem
import meta.snapshot

class GDFSErrorCode:
    pass

class GDFSJSONKey:
    ID    = u'id'
    ITEMS = u'items'

class GDFS(bakfilesystem.BackupFileSystem):
    """An implementation for storage operation on google drive."""
    ID = "googledrive"
    SEPERATOR   = "/"

    # service related constants
    SCOPE = "https://www.googleapis.com/auth/drive"

    # constructor
    def __init__(self, configure, decorator, DEBUG=False):
        # super
        bakfilesystem.BackupFileSystem.__init__(self, DEBUG)

        GDFS.DEBUG = DEBUG
        self.configure = configure
        self.decorator = decorator
        self.rootpath  = ""

        # get credential
        client_id     = self.configure["CLIENT_ID"]
        client_secret = self.configure["CLIENT_SECRET"]
        flow = oauth2client.client.OAuth2WebServerFlow(client_id, \
               client_secret, GDFS.SCOPE)
        credential = self._get_credential(flow, self.configure["CREDENTIAL"])
        if GDFS.DEBUG:
            print "[DEBUG] Get credential for access to Google Drive done."

        # authorize service
        http = httplib2.Http()
        http = credential.authorize(http)
        # we need this authenticated handle to retrieve data
        self.http = http

        self.service  = apiclient.discovery.build('drive', 'v2', http=http)

        # on google drive there is no concept like container, etc.
        # just setup required directories
        # one for snapshot and tag the other and the id should be recorded
        # since directory name has no relation with sub-sequent operations
        # directory names can be obfuscated to hide details
        # however, in that policy, we need to modify configuration file
        self.ss_folder  = self._create_folder_if_not_exists(u"ss")
        self.tag_folder = self._create_folder_if_not_exists(u"t")

        if GDFS.DEBUG:
            print "[DEBUG] GDFS initialization done."

    def list_snapshots(self):
        """Get all named snapshots"""
        if GDFS.DEBUG:
            print "[DEBUG] Check all available snapshots."
        # the result id time mapping
        ss_dict = {}
        try:
            # in case the folder is empty
            res = self._find("'%s' in parents" % self.ss_folder)
            # for each record in result construct the id timestamp mapping
            for record in res[GDFSJSONKey.ITEMS]:
                ss_dict[record['title']] = record['modifiedDate']

        except apiclient.errors.HttpError:
            # File not found
            # ignore
            pass

        if GDFS.DEBUG:
            print "[DEBUG] All available snapshots:", ss_dict

        return ss_dict

    def get_snapshot(self, ss_id):
        """Get specific snapshot by snapshot id."""
        if GDFS.DEBUG:
            print "[DEBUG] Get snapshot:", ss_id

        resource = self._find("title='%s' and '%s' in parents" % \
                       (ss_id, self.ss_folder))
        try:
            url = resource['items'][0]['downloadUrl']
        except KeyError:
            # no such key
            # ignore
            print "error!!!"

            return None

        # a tuple will be returned
        # the first field is a json-formatted file metadata
        # the second field is content of the object
        rsp  = self.http.request(url)
        data = rsp[1]
        snapshot = meta.snapshot.SnapShot(self.decorator.undecorate(data))

        return snapshot

    def append_snapshot(self, ss, ss_id = ""):
        """Store snapshot given a snapshot object."""
        data = str(ss)

        if not len(data):
            md5   = hashlib.md5()
            md5.update(data)
            ss_id = md5.hexdigest()

        if GDFS.DEBUG:
            print "[DEBUG] Append snapshot:", ss_id
        data = self.decorator.decorate(data)

        # get metadata
        body = self._get_http_body(ss_id, "application/otcet-stream", \
                   [self.ss_folder])
        # use in memory stream
        media_body = apiclient.http.MediaInMemoryUpload(data)
        # upload file onto cloud
        self.service.files().insert(body=body, \
            media_body=media_body).execute()

        return ss_id

    def remove_snapshot(self, ss_id):
        """Remove specified snapshot."""
        if GDFS.DEBUG:
            print "[DEBUG] Remove snapshot:", ss_id

        resource = self._find("title='%s' and '%s' in parents" % \
                       (ss_id, self.ss_folder))
        sid = resource['items'][0]['id']
        # remove specified snapshot
        self.service.files().delete(sid).execute()

    def list_tags(self):
        """List all available tags."""
        if GDFS.DEBUG:
            print "[DEBUG] Check all available tags."

        tags = []
        try:
            # in case the folder is empty
            res = self._find("'%s' in parents" % self.tag_folder)
            # for each record in result construct the id timestamp mapping
            for record in res:
                tags.append(record['title'])

        except apiclient.errors.HttpError:
            # File not found
            # ignore
            pass

        if GDFS.DEBUG:
            print "[DEBUG] All available tags:", tags

        return tags

    def tag_snapshot(self, tag_obj):
        """Tag specified snapshot and path with given name."""
        if GDFS.DEBUG:
            print "[DEBUG] New tag:", tag_obj.tag_id, "on path", tag_obj.pname

        data  = self.decorate(str(tag_obj))
        body  = self._get_http_body(tag.tag_id, [self.tag_folder])
        media = apiclient.http.MediaInMemoryUpload(data)
        self.service.files().insert(body=body, media_body=media).execute()

    def get_tagged_snapshot(self, tag_id):
        if GDFS.DEBUG:
            print "[DEBUG] Get tag:", tag_id

        resource = self._find("title='%s' and '%s' in parents" % \
                       (tag_id, self.tag_folder))
        try:
            url = resource['items'][0]['downloadUrl']
        except KeyError:
            # no such key
            # ignore
            print "error!!!"
            return None

        # a tuple will be returned
        # the first field is a json-formatted file metadata
        # the second field is content of the object
        response, data  = self.http.request(url)
        tag  = meta.tag.Tag(self.decorator.undecorate(content))

        return tag

    def remove_tag(self, tag_id):
        if GDFS.DEBUG:
            print "[DEBUG] Remove tag:", tag_id

        resource = self._find("title='%s' and '%s' in parents" % \
                       (tag_id, self.tag_folder))
        tid = resource['items'][0]['id']
        # remove specified snapshot
        self.service.files().delete(tid).execute()

    def retrieve_to_file(self, obj_id, path):
        if GDFS.DEBUG:
            print "[DEBUG] Get object:", obj_id, "to", path

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # empty file, touch and truncate
            file(path, 'w')
        else:
            tmp_path = self.configure["SYS_DIR"] + "/tmp"
            resource = self._find("title='%s'" % obj_id)
            url      = resource['items'][0]['downloadUrl']
            # construct an HttpRequest object from scratch
            request  = apiclient.http.HttpRequest(self.http, None, url, headers={})
            fh       = io.FileIO(tmp_path, 'wb')
            dloader  = apiclient.http.MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = dloader.next_chunk()
            fh.close()

            self.decorator.undecorate_file(tmp_path, path)
            # remove temporary file
            os.unlink(tmp_path)

    def store_from_file(self, path, id=""):
        """Upload file onto cloud."""
        # get object id
        obj_id = \
            bakfilesystem.BackupFileSystem.store_from_file(self, \
                path, id)

        tmp_path = self.configure["SYS_DIR"] + "/tmp"
        self.decorator.decorate_file(path, tmp_path)

        if GDFS.DEBUG:
            print "[DEBUG] Store file:", path, "as", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # file not empty
            media = apiclient.http.MediaFileUpload(tmp_path, \
                mimetype="application/octet-stream")
            body  = self._get_http_body(obj_id)
            self.service.files().insert(body=body, media_body=media).execute()

            # remove temporary file
            os.unlink(tmp_path)

        return obj_id

    def store(self, data, id=""):
        """Store data directly onto cloud."""
        obj_id = bakfilesystem.BackupFileSystem.store(self, data, id)
        if GDFS.DEBUG:
            print "[DEBUG] Store data:", obj_id

        data = self.decorator.decorate(data)

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            body  = self._get_http_body(obj_id)
            media = apiclient.http.MediaInMemoryUpload(data)
            self.service.files().insert(body=body, media_body=media).execute()

        return obj_id

    def retrieve(self, obj_id):
        if GDFS.DEBUG:
            print "[DEBUG] Get object:", obj_id

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            data = ""
        else:
            resource = self._find("title='%s'" % obj_id)
            try:
                url      = resource['items'][0]['downloadUrl']
            except KeyError:
                print "error!!!"
                return
            (response, data) = self.http.request(url)
        # undecorate data
        data = self.decorator.undecorate(data)

        return data

    def remove(self, obj_id):
        """Remove object given an object ID."""
        if GDFS.DEBUG:
            print "[DEBUG] Remove object:", obj_id

        resource = self._find("title='%s'" % obj_id)
        oid = resource[GDFSJSONKey.ITEMS][0][GDFSJSONKey.ID]
        # remove specified snapshot
        self.service.files().delete(oid).execute()

    def _get_download_url(self, obj_id):
        """Get object download URL from object id."""
        pass

    def _find(self, query_condition):
        return self.service.files().list(q="%s" % query_condition).execute()

    def _get_credential(self, flow, cred_file):
        # try get credential from credential file
        storage    = oauth2client.file.Storage(cred_file)
        credential = storage.get()

        # no credential file exists
        if credential is None or credential.invalid:
            credential = oauth2client.tools.run(flow, storage)

        return credential

    def _get_http_body(self, title=u"", mimeType="", parents=[]):
        body = {}
        # set title of body
        if len(title):
            body['title']    = title
        else:
            print "Empty title in get http body: error!!!"

        # set mime type
        if len(mimeType):
            body['mimeType'] = mimeType

        # set all parents
        body['parents'] = []
        for parent in parents:
            body['parents'].append({'id': parent})

        return body

    def _create_folder_if_not_exists(self, folder_name):
        """Create a folder with specified folder name.

Return:
    folder id."""
        results = \
           self.service.files().list(q=u"title='%s'" % \
               folder_name).execute()
        items = results[GDFSJSONKey.ITEMS]

        if not len(items):
            # folder not created yet
            body = self._get_http_body(folder_name, \
                       "applicatoin/vnd.google-apps.folder")
            # execute immediately
            fid = self.service.files().insert(body=body).execute()[GDFSJSONKey.ID]
        else:
            # the invarant ensure unique-ness of folder name
            fid = items[0][GDFSJSONKey.ID]

        return fid

    def get_empty_data_md5(self):
        md5 = hashlib.md5()
        md5.update(self.decorator.decorate(""))

        return md5.hexdigest()
