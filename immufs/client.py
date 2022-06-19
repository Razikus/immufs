from dataclasses import dataclass
from immudb.client import ImmudbClient
from immudb.datatypes import DeleteKeysRequest, KeyValue
from pathlib import PurePath
from enum import Enum, IntEnum
from typing import List

class ErrorCode(IntEnum):
    DIRECTORY_NOT_EXISTS = 0
    SOURCE_FILE_NOT_EXISTS = 1
    TARGET_FILE_IS_DIRECTORY = 2

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
        return PurePath(filePath).as_posix().encode("utf-8")

    def _getDirectoryKey(self, filePath: str):
        return PurePath(filePath).as_posix().replace("/", "|").encode("utf-8")
    
    def _getFileSettingKey(self, filePath: str):
        return PurePath(filePath).as_posix().replace("/", "\\").encode("utf-8")
    
    def list_directory(self, where = "/") -> List[PurePath]:
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
        dirKey = self._getDirectoryKey(where)
        lastScan = self.client.scan(lastKey, self._getDirectoryKey(where), False, 1000)
        while lastScan:
            for item in lastScan.keys():
                decoded = item.decode("utf-8")
                if(item == dirKey):
                    continue
                splitted = decoded.split(where.replace("/", "|"))
                if(len(splitted) == 2):
                    keys.append(PurePath(where).joinpath(PurePath(decoded.replace("|", "/"))))
            lastScan = self.client.scan(list(lastScan.keys())[-1], self._getDirectoryKey(where), False, 1000)
        return keys

    def remove(self, filePath: str):
        self.ensureLogged()
        isFile, error = self.isFile(PurePath(filePath))
        if(isFile):
            deleteRequest = DeleteKeysRequest(keys = [self._getFileKey(filePath)])
            self.client.delete(deleteRequest)
            return True, None
        isDir, error = self.isDirectory(PurePath(filePath))
        if(isDir):
            deleteRequest = DeleteKeysRequest(keys = [self._getDirectoryKey(filePath)])
            self.client.delete(deleteRequest)
            return True, None

        return False, error

    def isDirectory(self, directoryPath: PurePath):
        self.ensureLogged()
        if(directoryPath.as_posix() == "/"):
            return b'777', None
        else:
            directoryKey = self._getDirectoryKey(directoryPath.as_posix())
            what = self.client.get(directoryKey)
            if(what):
                return True, None
            return False, None

    def isFile(self, filePath: PurePath):
        self.ensureLogged()
        value = self.client.get(self._getFileKey(filePath.as_posix()))
        if(value):
            return True, value
        else:
            return False, ErrorCode.SOURCE_FILE_NOT_EXISTS

    def createFile(self, filePath: str, content, offset: int = 0):
        print(filePath, content, offset)
        self.ensureLogged()
        path = PurePath(filePath)
        directory, error = self.isDirectory(path.parent)
        if(not directory):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        wholeContent = b''

        if(offset > 0):
            wholeContent = self.readFile(filePath)
            wholeContent = wholeContent[0:offset]
        readed = content.read()
        wholeContent = wholeContent + readed
        self.client.set(self._getFileKey(path.as_posix()), wholeContent)
        return True, None

    def createDirectory(self, filePath: str):
        self.ensureLogged()
        if(not filePath.endswith("/")):
            filePath = filePath + "/"
        path = PurePath(filePath)
        if(not self.isDirectory(path.parent)):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        self.client.set(self._getDirectoryKey(path.as_posix()), b'777')
        return True, None

    def readFile(self, filePath: str):
        self.ensureLogged()
        valueOf = self.client.get(self._getFileKey(filePath))
        print("XXXX", filePath)
        if(valueOf):
            return valueOf.value
        else:
            return None

    def deleteDirectory(self, directoryPath: str, recursive: bool = False):
        self.ensureLogged()
        pass

    def appendToFile(self, filePath: str, content):
        self.ensureLogged()
        pass

    def move(self, filePath: str, newPath: str):
        self.ensureLogged()
        valueOf = self.client.get(self._getFileKey(filePath))
        if(valueOf):

            deleteRequest = DeleteKeysRequest(keys = [self._getFileKey(filePath)])
            self.client.set(self._getFileKey(newPath), valueOf.value)
            self.client.delete(deleteRequest)
            return True, None
        else:
            return False, ErrorCode.SOURCE_FILE_NOT_EXISTS

    def overrideFile(self, filePath: str, content):
        self.ensureLogged()
        pass


