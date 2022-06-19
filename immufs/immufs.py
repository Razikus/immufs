#!/usr/bin/env python

#    Copyright (C) 2022 Codenotary 
#    Adam Raźniewski  <adam@codenotary.com>

from __future__ import print_function

import os, sys
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


if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')


def flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m


class ImmuFS(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)
        self.root = '/'
        self.client = ImmuFSClient('localhost', 3322)
        

    def getattr(self, path):
        print("getattr", path)
        return os.lstat("." + path)

    def readlink(self, path):
        print("readlink", path)
        return os.readlink("." + path)

    def readdir(self, path, offset):
        print("readdir", path, offset)
        print(os.listdir("." + path))
        for e in os.listdir("." + path):

            yield fuse.Direntry(e)

    def unlink(self, path):
        print("unlink", path)
        os.unlink("." + path)

    def rmdir(self, path):
        print("rmdir", path)
        os.rmdir("." + path)

    def symlink(self, path, path1):
        print("symlink", path)
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        print("rename", path, path1)
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        print("link", path, path1)
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        print("chmod", path, mode)
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        print("chown", path, user, group)
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        print("truncate", path, len)
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        print("mknod", path, mode, dev)
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        print("mkdir", path, mode)
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        print("utime", path, times)
        os.utime("." + path, times)

    def access(self, path, mode):
        print("access", path, mode)
        if not os.access("." + path, mode):
            return -EACCES

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
        return os.statvfs(".")

    def fsinit(self):
        print("fsinit")
        os.chdir(self.root)

    class XmpFile(object):

        def __init__(self, path, flags, *mode):
            print("INIT FILE XMP", path, flags, mode)
            self.file = os.fdopen(os.open("." + path, flags, *mode),
                                  flag2mode(flags))
            self.fd = self.file.fileno()
            if hasattr(os, 'pread'):
                self.iolock = None
            else:
                self.iolock = Lock()

        def read(self, length, offset):
            print("READ XML", length, offset)
            if self.iolock:
                self.iolock.acquire()
                try:
                    self.file.seek(offset)
                    return self.file.read(length)
                finally:
                    self.iolock.release()
            else:
                return os.pread(self.fd, length, offset)

        def write(self, buf, offset):
            print("write XML", buf, offset)
            if self.iolock:
                self.iolock.acquire()
                try:
                    self.file.seek(offset)
                    self.file.write(buf)
                    return len(buf)
                finally:
                    self.iolock.release()
            else:
                return os.pwrite(self.fd, buf, offset)

        def release(self, flags):
            print("release XML", flags)
            self.file.close()

        def _fflush(self):
            print("_fflush")
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            print("fsync", isfsyncfile)
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            print("XMP flush")
            self._fflush()
            # cf. xmp_flush() in fusexmp_fh.c
            os.close(os.dup(self.fd))

        def fgetattr(self):
            print("fgetattr")
            return os.fstat(self.fd)

        def ftruncate(self, len):
            print("ftruncate", len)
            self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            print("lock", cmd, owner, kw)
            # The code here is much rather just a demonstration of the locking
            # API than something which actually was seen to be useful.

            # Advisory file locking is pretty messy in Unix, and the Python
            # interface to this doesn't make it better.
            # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
            # way. The following implementation *might* work under Linux. 
            #
            # if cmd == fcntl.F_GETLK:
            #     import struct
            # 
            #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
            #                            kw['l_start'], kw['l_len'], kw['l_pid'])
            #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
            #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
            #     uld2 = struct.unpack('hhQQi', ld2)
            #     res = {}
            #     for i in xrange(len(uld2)):
            #          res[flockfields[i]] = uld2[i]
            #  
            #     return fuse.Flock(**res)

            # Convert fcntl-ish lock parameters to Python's weird
            # lockf(3)/flock(2) medley locking API...
            op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
                   fcntl.F_RDLCK : fcntl.LOCK_SH,
                   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
            if cmd == fcntl.F_GETLK:
                return -EOPNOTSUPP
            elif cmd == fcntl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
            elif cmd == fcntl.F_SETLKW:
                pass
            else:
                return -EINVAL

            fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])


    def main(self, *a, **kw):

        self.file_class = self.XmpFile

        return Fuse.main(self, *a, **kw)


def main():

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.
""" + Fuse.fusage

    server = ImmuFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print("can't enter root of underlying filesystem", file=sys.stderr)
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()