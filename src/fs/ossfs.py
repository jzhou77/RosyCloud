# This is the file system module for OSS cloud service
# It constructs and maintains the file hierachies
import sys
import os
import dateutil.parser
import hashlib
import StringIO

# extended modules
import oss.oss_api
import oss.oss_xml_handler

import filesystem
import bakfilesystem
import meta.snapshot
import meta.tag

class OSSErrorCode:
    REQUEST_OK     = 200
    NO_SUCH_BUCKET = 404
    INVALID_BUCKET = 400

class OSSFS(bakfilesystem.BackupFileSystem):
    ID = "oss"
    SERVICE_URL = "oss.aliyuncs.com"
    SEPERATOR   = "/"
    TRIALS      = 3
    
    # constructor
    def __init__(self, configure, decorator, DEBUG=False):
        # super
        bakfilesystem.BackupFileSystem.__init__(self, DEBUG)

        OSSFS.DEBUG    = DEBUG
        self.configure = configure
        self.decorator = decorator
        self.oss       = oss.oss_api.OssAPI(OSSFS.SERVICE_URL,
            configure["ACCESS_ID"], configure["SECRET_ACCESS_KEY"])
        # no special delimeter to indicate root
        self.rootpath  = ""
        # snapshot folder used to distinguish snapshots and blocks
        self.ss_folder  = u"ss/"
        self.tag_folder = u"t/"

        # set bucket name
        try:
            OSSFS.BUCKET = self.configure["OSS_BUCKET"]
        except KeyError:
            print "OSS bucket is not set properly."
            print "Program exits."

            # bucket not specified
            sys.exit(-2)

        msg = self.oss.get_bucket_acl(OSSFS.BUCKET)
        # bucket exists? if not create it
        if not msg.status == OSSErrorCode.REQUEST_OK:
            if OSSFS.DEBUG:
                print "[DEBUG] Initialize cloud storage."

            # exit if create bucket fails after 3 trials
            trials = 1
            fail   = False
            msg = self.oss.create_bucket(OSSFS.BUCKET)
            while (not msg.status == OSSErrorCode.REQUEST_OK) and \
                (trials <= OSSFS.TRIALS):
                if trials == OSSFS.TRIALS:
                    fail = True
                else:
                    msg = self.oss.create_bucket(OSSFS.BUCKET)
                trials = trials + 1
            if fail:
                print "Cannot access oss storage. Program exit."
                sys.exit(-1)
            else:
                # create ss container
                self.oss.put_object_from_string(OSSFS.BUCKET, self.ss_folder, "")

    def list_snapshots(self):
        """Get all named snapshots"""
        # the result id time mapping
        ss_list = []
        res = self.oss.get_bucket(OSSFS.BUCKET, self.ss_folder)
        if (res.status / 100) == 2:
            body = res.read()
            (ss_meta, ignore) = oss.oss_xml_handler.GetBucketXml(body).list()

            # extract snapshot id
            for sse in ss_meta:
                if not sse[0] == self.ss_folder:
                    ss_list.append(sse[0][len(self.ss_folder):])

            return ss_list

    def get_snapshot(self, ss_id):
        """Get snapshot content."""
        obj_id = self._join(self.ss_folder, ss_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Get snapshot:", obj_id
        res = self.oss.get_object(OSSFS.BUCKET, obj_id)
        if res.status == OSSErrorCode.REQUEST_OK:
            data = self.decorator.undecorate(res.read())
            return meta.snapshot.SnapShot(data)
        else:
            raise IOError("SnapShot: " + ss_id)

    def get_snapshot_timestamp(self, ss_id):
        """Get snapshot timestamp."""
        obj_id = self._join(self.ss_folder, ss_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Get snapshot timestamp:", obj_id
        res = self.oss.head_objcet(OSSFS.BUCKET, obj_id)
        if (res.status / 100) == 2:
            return datetime.datetime.strptime( \
                res.getheaders['last-modified'], \
                '%a, %d %b %y %H:%M:%S GMT')
        else:
            raise IOError("Snapshot timestamp:", obj_id)

    # all the snapshots are put into folder 'ss/' on cloud
    def append_snapshot(self, ss, ss_id = ""):
        data = str(ss)
        if not len(ss_id):
            md5   = hashlib.md5()
            md5.update(data)
            ss_id = md5.hexdigest()

        obj_id = self._join(self.ss_folder, ss_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Append snapshot:", obj_id
        # decorate data after object id done
        data = self.decorator.decorate(data)
        # a snapshot cannot be empty, put data directly
        msg = self.oss.put_object_from_string(OSSFS.BUCKET, obj_id, \
            data, content_type = 'application/octet-stream')
        # return md5 checksum if put successfully
        if not msg.status == OSSErrorCode.REQUEST_OK:
           raise IOError(obj_id)

        return ss_id

    def remove_snapshot(self, ss_id):
        obj_id = self._join(self.ss_folder, ss_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Remove snapshot:", obj_id
        msg = self.oss.delete_object(OSSFS.BUCKET, obj_id)

    def list_tags(self):
        tags = []
        res = self.oss.get_bucket(OSSFS.BUCKET, self.tag_folder)
        if (res.status / 100) == 2:
            body = res.read()
            (tag_list, ignore) = oss.oss_xml_handler.GetBucketXml(body).list()

            # extract snapshot id to timestamp mapping
            for tag in tag_list:
                tags.append([sse[0][len(self.tag_folder):]])

        return tags

    # the interface should be change to accept a tag object directly
    # the underlying storage should have no sense to tag object
    # structure
    def tag_snapshot(self, tag_id, ss_id, path):
        """Tag specified snapshot and path with given name."""
        tag = meta.tag.Tag()
        tag.ss_id = ss_id
        tag.pname = path.decode('utf-8').encode('utf-8')

        obj_id = self._join(self.tag_folder, tag_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Tag snapshot on path:", obj_id, "@", path
        data = self.decorator.decorate(str(tag))
        # a snapshot cannot be empty, put data directly
        msg = self.oss.put_object_from_string(OSSFS.BUCKET, obj_id, \
            data, content_type = 'application/octet-stream')
        # return md5 checksum if put successfully
        if not msg.status == OSSErrorCode.REQUEST_OK:
           raise IOError(obj_id)

    def get_tagged_snapshot(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Get tag:", obj_id
        res = self.oss.get_object(OSSFS.BUCKET, obj_id)
        if res.status == OSSErrorCode.REQUEST_OK:
            data = self.decorator.undecorate(res.read())
            return meta.tag.Tag(data)
        else:
            raise IOError("Tag: " + tag_id)

    def remove_tag(self, tag_id):
        obj_id = self._join(self.tag_folder, tag_id)
        if OSSFS.DEBUG:
            print "[DEBUG] Remove tag:", obj_id
        msg = self.oss.delete_object(OSSFS.BUCKET, obj_id)
            
    def retrieve_to_file(self, obj_id, path):
        if OSSFS.DEBUG:
            print "[DEBUG] Get object:", obj_id, "to", path

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # empty file, touch and truncate it
            file(path, 'w')
        else:
            tmp_path = self.configure["SYS_DIR"] + "/tmp/" + obj_id
            msg = self.oss.get_object_to_file(OSSFS.BUCKET, obj_id, \
                tmp_path)
            if not msg.status == OSSErrorCode.REQUEST_OK:
                raise IOError(obj_id)
            else:
                self.decorator.undecorate_file(tmp_path, path)
                # remove temporary file
                os.unlink(tmp_path)
        
    # store data
    def store_from_file(self, path, id=""):
        # get object id
        obj_id = \
            bakfilesystem.BackupFileSystem.store_from_file(self, path, id)

        tmp_path = self.configure["SYS_DIR"] + "/tmp/" + obj_id
        self.decorator.decorate_file(path, tmp_path)

        if OSSFS.DEBUG:
            print "[DEBUG] Store file:", path, "as", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # file not empty
            msg = self.oss.put_object_from_file(OSSFS.BUCKET, obj_id, \
                tmp_path, content_type = 'application/octet-stream')
            # return md5 checksum if put successfully
            if not msg.status == OSSErrorCode.REQUEST_OK:
                raise IOError(obj_id)

        # remove temporary file
        os.unlink(tmp_path)

        return obj_id

    def store(self, data, id=""):
        obj_id = bakfilesystem.BackupFileSystem.store(self, data, id)
        if OSSFS.DEBUG:
            print "[DEBUG] Store object:", obj_id

        if not obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # file not empty
            data = self.decorator.decorate(data)
            msg  = self.oss.put_object_with_data(OSSFS.BUCKET, obj_id, \
                data, content_type = 'application/octet-stream')
            # return md5 checksum if put successfully
            if not msg.status == OSSErrorCode.REQUEST_OK:
                raise IOError(obj_id)

        return obj_id

    def retrieve(self, obj_id):
        if OSSFS.DEBUG:
            print "[DEBUG] Get object:", obj_id

        if obj_id == filesystem.FileSystem.EMPTY_FILE_MD5:
            # empty object
            data = ""
        else:
            msg = self.oss.get_object(OSSFS.BUCKET, obj_id)
            if msg.status == OSSErrorCode.REQUEST_OK:
                data = msg.read()
                data = self.decorator.undecorate(data)
            else:
                raise IOError(obj_id)

        return data
        
    def remove(self, obj_id):
        if OSSFS.DEBUG:
            print "[DEBUG] Remove object:", obj_id
        msg = self.oss.delete_object(OSSFS.BUCKET, obj_id)

    def _join(self, base, rel):
        if not base[-1] == OSSFS.SEPERATOR:
            base = base + OSSFS.SEPERATOR

        return base + rel
