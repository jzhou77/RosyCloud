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

# This module including garbage collection related topic for rosycloud.
# class GarbageCollector achieves all the functionalities.

import sys
import os
import datetime

import rosycloud
# tree snapshot
# and find file system hierachy
import fs.filesystem

def total_seconds(td):
    seconds = (td.microseconds + \
        (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

    return seconds

def parents_changed(ps1, ps2):
    return (len(set(ps1) - set(ps2)) or len(set(ps2) - set(ps1)))

class GarbageCollector:
    """Base class and sample implementation of a garbage collector.
It can be extended by re-implementing required methods."""
    KEEP_ONE      = 1
    KEEP_LANDMARK = 2

    # we keep all the latest 500 modifications.
    # will be longened after debug complete.
    SHORT_TERM_VER_RANGE = 0
    # long term stable time range, 1000s
    # will be longened after debug complete.
    LONG_TERM_TIME_DELTA = 3600

    def __init__(self, configure, localfs, keep_one, clouds):
        """Initialize a garbage collector object.
Params:
    configure: system-wise configuration.
    clouds: all the clouds supported."""
        self.configure = configure
        self.localfs   = localfs
        self.clouds = clouds
        if keep_one:
            self.policy = GarbageCollector.KEEP_ONE
        else:
            self.policy = GarbageCollector.KEEP_LANDMARK

    def gc_all(self):
        """Collect garbages on all supported clouds."""
        # carbage collect on each cloud
        for cloud in self.clouds:
            # first synchronize the cloud to eliminate branches
            rosycloud.sync(cloud, self.localfs)
            if self.policy == GarbageCollector.KEEP_ONE:
                self.gc_latest(cloud)
            elif self.policy == GarbageCollector.KEEP_LANDMARK:
                self.gc(cloud)

        # remove all local caches
        # self.empty_cache()

    def gc(self, cloud):
        """Collect garbage on specified cloud with keep landmark.
Params:
    cloud: an instance of BackupFileSystem, whose garbage should be
           collected and deprecated storage be reclaimed."""
        (root, snapshots) = fs.filesystem.tree_snapshot(cloud)
        try:
            print root
            assert(len(root) == 1)
            head = root[0]
        except AssertionError:
            return rosycloud.SYS_ASSERT_FAIL

        # snapshot need to be deleted
        landmarks = [head]
        # the following snapshot, if timestamp stable in a range, keep it
        while head:
            pre = head
            snapshot = snapshots[head]
            # if snapshot is not marked as important
            if len(snapshot.parents) > 1:
                head = rosycloud.find_lowest_common_ancestor( \
                       snapshot.parents, snapshots)
            elif len(snapshot.parents) == 1:
                head = snapshot.parents[0]
            else:
                # end of chain
                break

            snapshot = snapshots[head]
            if snapshot.marked():
                landmarks.append(head)
            else:
                # if snapshot be stable for relative long time,
                # annotate as landmark
                if (total_seconds(cloud.get_snapshot_timestamp(pre) - \
                        cloud.get_snapshot_timestamp(head))) > \
                        GarbageCollector.LONG_TERM_TIME_DELTA:
                    landmarks.append(head)

        # remove all pruned snapshot
        for snapshot in snapshots:
            if snapshot not in landmarks:
                self.localfs.remove_snapshot(snapshot)
                cloud.remove_snapshot(snapshot)

        self.prune_cloud(cloud, landmarks)

    def gc_latest(self, cloud):
        """Collect garbage on specified cloud with keep one policy.
Params:
    cloud: an instance of BackupFileSystem, whose garbage should be
           collected and deprecated storage be reclaimed."""
        (root, snapshots) = fs.filesystem.tree_snapshot(cloud)
        try:
            assert(len(root) == 1)
        except AssertionError:
            # assertion error, return corresponding code
            return rosycloud.SYS_ASSERT_FAIL

        root = root[0]
        for snapshot in snapshots:
            if not snapshot == root:
                cloud.remove_snapshot(snapshot)
                self.localfs.remove_snapshot(snapshot)

        prune_cloud(cloud, [root])

    def prune_cloud(self, cloud, landmarks = {}):
        """Prune a cloud according to a pruned snapshot list.
Params:
    cloud: cloud want to prune;
    prouned_ss_list: pruned snapshot list.
                     By default, the snapshot list is empty,
                     an extra query for the snapshot list on cloud is
                     required, increasing the network burden."""
        # no landmark given?
        # request again
        if not len(landmarks):
            landmarks = fs.filesystem.tree_snapshot(cloud)

        # all the objects on cloud
        objects  = cloud.list_objects()
        referred = []

        # for each snapshot, record all referred objects
        parent = []
        # we need the parental relationship, the connect is reversed
        landmarks.reverse()
        # mark objects referenced by a snapshot
        for landmark in landmarks:
            snapshot = cloud.get_snapshot(landmark)
            hier = fs.filesystem.hierachy(snapshot.root,cloud,self.localfs)
            stck = [snapshot.root]
            while len(stck):
                entry = stck.pop()
                referred.append(entry.obj_id)
                if entry.isdir():
                    dir_obj = hier[entry.obj_id]
                    for e in dir_obj.dir_entries:
                        if not e == fs.meta.dir.Dir.SELF_REF:
                            stck.append(dir_obj.dir_entries[e])

            # update snapshot chain
            if snapshot.marked():
                # snapshot already landmarked
                parent = [landmark]
            else:
                snapshot.mark()
                snapshot.set_parents(parent)
                ss_id  = cloud.append_snapshot(snapshot)
                self.localfs.append_snapshot(snapshot, ss_id)
                parent = [ss_id]
                cloud.remove_snapshot(landmark)
                self.localfs.remove_snapshot(landmark)

        # change to new root
        if not len(parent):
            parent = landmarks[0]
        else:
            parent = parent[0]
        self.localfs.set_root_snapshot_id(parent)

        # reclaim all the storage for non-referred objects
        for obj in objects:
            if obj not in referred:
               self.localfs.remove(obj)
               cloud.remove(obj)

    # def empty_cache(self)

# entrance for this module
def main(conf, localfs, keep_one, clouds):
    """Params:
    conf: system wide configuration,
    keep_one: boolean variable indicating which policy in use;
              keep latest if yes, keep landmark otherwise;
    clouds: all clouds where garbage collection should be performed on."""
    GarbageCollector(conf, localfs, keep_one, clouds).gc_all()
