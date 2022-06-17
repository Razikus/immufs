from dataclasses import dataclass
from immudb.client import ImmudbClient
from pathlib import PurePath
from enum import Enum, IntEnum

class ErrorCode(IntEnum):
    DIRECTORY_NOT_EXISTS = 0

class ImmuFSClient:
    def __init__(self, host, port):
        self.client = ImmudbClient(f"{host}:{port}")
        self.host = host
        self.port = port

    def ensureLogged(self):
        self.client.login("immudb", "immudb", "defaultdb")

    def _defaultEncode(self, what: str):
        return what.encode("utf-8")

    def _getFileKey(self, filePath: str):
        return filePath.encode("utf-8")

    def _getDirectoryKey(self, filePath: str):
        return filePath.replace("/", "|").encode("utf-8")
    
    def _getFileSettingKey(self, filePath: str):
        return filePath.replace("/", "\\").encode("utf-8")
    
    def list_directory(self, where = "/"):
        self.ensureLogged()
        lastKey = b''
        lastScan = self.client.scan(lastKey, self._defaultEncode(where), False, 1000)
        keys = []
        while lastScan:
            for item in lastScan.keys():
                decoded = item.decode("utf-8")
                splitted = decoded.split(where)
                if(len(splitted) == 2):
                    keys.append(PurePath(item.decode("utf-8")))
            lastScan = self.client.scan(list(lastScan.keys())[-1], self._defaultEncode(where), False, 1000)
        

        lastKey = b''
        lastScan = self.client.scan(lastKey, self._getDirectoryKey(where), False, 1000)
        while lastScan:
            for item in lastScan.keys():
                decoded = item.decode("utf-8")
                splitted = decoded.split(where.replace("/", "|"))
                if(len(splitted) == 2):
                    keys.append(PurePath(where).joinpath(PurePath(item.decode("utf-8").replace("|", "/"))))
            lastScan = self.client.scan(list(lastScan.keys())[-1], self._getDirectoryKey(where), False, 1000)
        return keys

    def remove(self, filePath: str):
        pass

    def isDirectory(self, directoryPath: PurePath):
        if(directoryPath.as_posix() == "/"):
            return b'777', None
        else:
            directoryKey = self._getDirectoryKey(directoryPath.as_posix())
            what = self.client.get(directoryKey)
            if(what):
                return True
            return False

    def createFile(self, filePath: str, content):
        self.ensureLogged()
        path = PurePath(filePath)
        if(not self.isDirectory(path.parent)):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        readed = content.read(1024)
        wholeContent = readed
        while readed:
            readed = content.read(1024)
            wholeContent = wholeContent + readed
        self.client.set(self._getFileKey(path.as_posix()), wholeContent)
        return True, None

    def createDirectory(self, filePath: str):
        self.ensureLogged()
        path = PurePath(filePath)
        if(not self.isDirectory(path.parent)):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        self.client.set(self._getDirectoryKey(path.as_posix()), b'777')
        return True, None

    def deleteDirectory(self, directoryPath: str):
        pass

    def appendToFile(self, filePath: str, content):
        pass

    def move(self, filePath: str, newPath: str):
        pass

    def overrideFile(self, filePath: str, content):
        pass


