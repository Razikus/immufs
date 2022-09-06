#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#    Copyright (C) 2022 Codenotary 
#    Adam Raźniewski  <adam@codenotary.com>

from __future__ import print_function
from genericpath import isfile

import os, sys, stat, errno
from pathlib import PurePath
from errno import *
from stat import *
import fcntl
from threading import Lock
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse
from .client import ImmuFSClient
from io import BytesIO


if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')

class ImmuStatVFS:
    def __init__(self):
        self.f_bsize=40960
        self.f_frsize=40960
        self.f_blocks=61664815
        self.f_bfree=12768427
        self.f_bavail=9618616
        self.f_files=15728640
        self.f_ffree=10849955
        self.f_favail=10849955
        self.f_flag=4096
        self.f_namemax=255

class ImmuStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

immufsClient = None
class ImmuFS(Fuse):

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        

    def getattr(self, path):
        isDir = immufsClient.getDirectoryByPath(PurePath(path))
        if(isDir):
            st = ImmuStat()
            st.st_mode = isDir.flags
            st.st_nlink = 2
            return st

        isFile = immufsClient.getFileMeta(PurePath(path))
        if(isFile):
            st = ImmuStat()
            st.st_mode = isFile.flags
            st.st_nlink = 1
            st.st_size = isFile.filesize
            return st

        return -errno.ENOENT

    def readlink(self, path):
        print("readlink", path)
        return os.readlink("." + path)

    def readdir(self, path, offset):
        dirs = immufsClient.list_directory(PurePath(path).as_posix())
        for e in dirs:
            yield fuse.Direntry(e.name)

    def unlink(self, path):
        immufsClient.remove(path)

    def rmdir(self, path):
        immufsClient.remove(path)

    def symlink(self, path, path1):
        print("symlink", path)
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        print("rename", path, path1)
        immufsClient.move(path, path1)

    def link(self, path, path1):
        print("link", path, path1)
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        fileFrom = immufsClient.getFileMeta(path)
        if fileFrom:
            immufsClient.updateFileFlags(fileFrom.uniqueid, mode)
        else:
            isDir = immufsClient.getDirectoryUUID(path)
            if isDir:
                immufsClient.updateDirectoryFlags(isDir, mode)

        print("chmod", path, mode)

    def chown(self, path, user, group):
        print("chown", path, user, group)

    def truncate(self, path, len):
        immufsClient.createFile(path, BytesIO(b''))
        # print("truncate", path, len)
        # f = open("." + path, "a")
        # f.truncate(len)
        # f.close()

    def mknod(self, path, mode, dev):
        print("mknod", path, mode, dev)
        # os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        immufsClient.createDirectory(PurePath(path).as_posix(), 16384 | mode)
        # print("mkdir", path, mode)
        # os.mkdir("." + path, mode)

    def utime(self, path, times):
        print("utime", path, times)
        # os.utime("." + path, times)

    def access(self, path, mode):
        print("access", path, mode)
        # if not os.access("." + path, mode):
        #     return -EACCES

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.
        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:
            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        print("statfs")
        return ImmuStatVFS()

    def fsinit(self):
        pass
        

    class XmpFile(object):

        def __init__(self, path, flags, *mode):
            self.path = path
            self.flags = flags
            self.mode = mode
            self.readedFileCache = None
            self.readedFileCacheMeta = None
            self.readedFileLength = -1
            self.writeBuf = None
            self.tooBig = False

        def read(self, length, offset):
            if(self.readedFileCache == None):
                self.readedFileCacheMeta = immufsClient.getFile(self.path)
                self.readedFileCache = self.readedFileCacheMeta.content
                self.readedFileLength = length
                return self.readedFileCache[offset: offset + length]
            else:
                self.readedFileLength = self.readedFileLength + length
                return self.readedFileCache[offset: offset + length]

        def write(self, buf, offset):
            if(self.tooBig):
                return -1
            if(self.writeBuf == None):
                self.writeBuf = BytesIO()
            # if(self.writeBuf.tell() >= (4194304 - len(buf))):
            #     self.writeBuf = None
            #     self.tooBig = True
            #     return 0
            self.writeBuf.write(buf)
            return(len(buf))

        def release(self, flags):
            print("release XML", flags)

        def _fflush(self):
            print("_fflush")

        def fsync(self, isfsyncfile):
            print("fsync", isfsyncfile)
            self._fflush()

        def flush(self):
            print("XMP flush")
            if(self.writeBuf):
                self.writeBuf.seek(0)
                immufsClient.createFile(self.path, self.writeBuf, self.mode, 0)
                self.writeBuf = None
            self._fflush()

        def fgetattr(self):
            st = ImmuStat()
            if(self.mode and len(self.mode) > 0):
                st.st_mode = self.mode[0]
            elif(self.readedFileCacheMeta != None):
                st.st_mode = self.readedFileCacheMeta.flags
            st.st_nlink = 1
            if(self.readedFileCache):
                st.st_size = len(self.readedFileCache)
            else:
                st.st_size = 0
            return st

        def ftruncate(self, len):
            immufsClient.createFile(self.path, BytesIO(b''))

        def lock(self, cmd, owner, **kw):
            print("lock", cmd, owner, kw)


    def main(self, *a, **kw):
        global immufsClient
        parsed = self.parser.parse_args()[0]
        url, port = parsed.serverurl.split(':')
        print("IMMUFS Starting!")
        immufsClient = ImmuFSClient(url, int(port), parsed.login, parsed.password, parsed.database)

        self.file_class = self.XmpFile

        return Fuse.main(self, *a, **kw)


def main():

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.
""" + Fuse.fusage

    server = ImmuFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="serverurl", metavar="SERVER", default='localhost:3322',
                             help="immudb server url [default: %default]")

    server.parser.add_option(mountopt="login", metavar="LOGIN", default='immudb',
                             help="immudb server login [default: %default]")

    server.parser.add_option(mountopt="password", metavar="PASSWORD", default='immudb',
                             help="immudb server password [default: %default]")

    server.parser.add_option(mountopt="database", metavar="DATABASE", default='defaultdb',
                             help="immudb server database [default: %default]")

    server.parse(values=server, errex=1)

    server.main()


if __name__ == '__main__':
    main()