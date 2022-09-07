from dataclasses import dataclass
import datetime
from uuid import uuid4
from immudb.client import ImmudbClient
from immudb.constants import COLUMN_NAME_MODE_FIELD
from immudb.datatypes import DeleteKeysRequest, KeyValue
from pathlib import PurePath
from enum import Enum, IntEnum, unique
from typing import List
import time

class ErrorCode(IntEnum):
    DIRECTORY_NOT_EXISTS = 0
    SOURCE_FILE_NOT_EXISTS = 1
    TARGET_FILE_IS_DIRECTORY = 2
    TARGET_DIRECTORY_IS_FILE = 3
@dataclass
class Directory:
    uniqueid: str
    name: str
    parent: str
    creationdate: datetime.datetime
    flags: int

@dataclass
class ImmuFile:
    uniqueid: str
    name: str
    directory: str
    creationdate: datetime.datetime
    flags: int
    content: bytes
    filesize: int

@dataclass
class FileMeta:
    uniqueid: str
    name: str
    flags: int
    filesize: int
    

class ImmuFSClient:
    def __init__(self, host, port, login, password, database):
        self.client = ImmudbClient(f"{host}:{port}")
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.database = database
        self.sqlTables = [
            """CREATE TABLE IF NOT EXISTS directories(
                uniqueid VARCHAR[32],
                name VARCHAR NOT NULL,
                parent VARCHAR[32],
                creationdate TIMESTAMP,
                flags INTEGER,
                PRIMARY KEY(uniqueid)
            )
            """,
            """CREATE TABLE IF NOT EXISTS files(
                uniqueid VARCHAR[32],
                name VARCHAR NOT NULL,
                directory VARCHAR[32],
                filesize integer,
                creationdate TIMESTAMP,
                flags INTEGER,
                content BLOB,
                PRIMARY KEY(uniqueid)
            )
            """
        ]
        self.initialize()

    def initialize(self):
        self.ensureLogged()
        for item in self.sqlTables:
            self.client.sqlExec(item)
        try:
            self.addDirectory("/", None, flags = 16384)
        except:
            pass

    def ensureLogged(self):
        self.client.login(self.login, self.password, self.database)

    def _defaultEncode(self, what: str):
        return what.encode("utf-8")
    
    def getDirectoryFiles(self, uuid):
        self.ensureLogged()
        allFiles = []
        pathOfDirectory = self.getDirectoryPath(uuid)
        files = self.client.sqlQuery("""SELECT uniqueid, name FROM files WHERE directory = @uuid LIMIT 999""", {"uuid": uuid}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        while files:
            lastuniqueid = files[-1]["uniqueid"]
            for file in files:
                allFiles.append(pathOfDirectory.joinpath(file["name"]))
            files = self.client.sqlQuery("""SELECT uniqueid, name FROM files WHERE directory = @uuid AND uniqueid > @lastuniqueid LIMIT 999""", {"uuid": uuid, "lastuniqueid":lastuniqueid}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        return allFiles

    def getFile(self, filePath: PurePath) -> ImmuFile:
        if(type(filePath) == str):
            filePath = PurePath(filePath)
        parent = filePath.parent
        directory = self.getDirectoryUUID(parent)
        name = filePath.name
        toRet = self.client.sqlQuery("""SELECT * FROM files where directory = @directory AND name = @name""", {"directory": directory, "name": name}, COLUMN_NAME_MODE_FIELD)
        if toRet:
            fileFrom = toRet[0]
            return ImmuFile(uniqueid = fileFrom["uniqueid"], name = fileFrom["name"], directory = fileFrom["directory"], creationdate = fileFrom["creationdate"], flags = fileFrom["flags"], content = fileFrom["content"], filesize=fileFrom["filesize"])
        else:
            return None

    def getFileUniqueUUID(self, filePath: PurePath):
        parent = filePath.parent
        directory = self.getDirectoryUUID(parent)
        name = filePath.name
        toRet = self.client.sqlQuery("""SELECT uniqueid, name FROM files where directory = @directory AND name = @name""", {"directory": directory, "name": name}, COLUMN_NAME_MODE_FIELD)
        if toRet:
            return toRet[0]["uniqueid"]
        else:
            return None
            
    def getFileMeta(self, filePath: PurePath):
        if(type(filePath) == str):
            filePath = PurePath(filePath)
        parent = filePath.parent
        directory = self.getDirectoryUUID(parent)
        name = filePath.name
        toRet = self.client.sqlQuery("""SELECT uniqueid, name, filesize, flags FROM files where directory = @directory AND name = @name""", {"directory": directory, "name": name}, COLUMN_NAME_MODE_FIELD)
        if toRet:
            return FileMeta(uniqueid = toRet[0]["uniqueid"], name = toRet[0]["name"], filesize = toRet[0]["filesize"], flags = toRet[0]["flags"])
        else:
            return None



    def getDirectoryDirectories(self, uuid):
        self.ensureLogged()
        allDirectories = []
        pathOfDirectory = self.getDirectoryPath(uuid)
        directories = self.client.sqlQuery("""SELECT uniqueid, name FROM directories WHERE parent = @uuid LIMIT 999""", {"uuid": uuid}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        while directories:
            lastuniqueid = directories[-1]["uniqueid"]
            for directory in directories:
                allDirectories.append(pathOfDirectory.joinpath(directory["name"]))
            directories = self.client.sqlQuery("""SELECT uniqueid, name FROM directories WHERE parent = @uuid AND uniqueid > @lastuniqueid LIMIT 999""", {"uuid": uuid, "lastuniqueid":lastuniqueid}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        return allDirectories

    def getDirectoryPath(self, uuid) -> PurePath:
        current = self.getDirectoryByUUID(uuid)
        path = [current.name]
        while current.parent != None:
            current = self.getDirectoryByUUID(current.parent)
            path.insert(0, current.name)
        allPath = "/".join(path) + "/"
        if(allPath.startswith("//")):
            allPath = allPath[1:]
        
        return PurePath(allPath)





    def list_directory(self, where = "/") -> List[PurePath]:
        self.ensureLogged()
        purePathFrom = PurePath(where)
        uniqueUuid = self.getDirectoryUUID(purePathFrom)

        keys = self.getDirectoryFiles(uniqueUuid)    
        keys.extend(self.getDirectoryDirectories(uniqueUuid))    

        return keys

    def deleteFile(self, fileUuid: str):
        self.client.sqlExec("""DELETE FROM files WHERE uniqueid = @uniqueid""", {"uniqueid": fileUuid})
        return True

    def deleteDirectory(self, directoryUuid: str):
        self.client.sqlExec("""DELETE FROM directories WHERE uniqueid = @uniqueid""", {"uniqueid": directoryUuid})
        return True

    def remove(self, filePath: str):
        self.ensureLogged()
        fileUuid = self.getFileUniqueUUID(PurePath(filePath))
        if(fileUuid):
            self.deleteFile(fileUuid)
            return True, None
        directoryUuid = self.isDirectory(PurePath(filePath))
        if(directoryUuid):
            self.deleteDirectory(directoryUuid)
            return True, None

        return False, None

    def isDirectory(self, directoryPath: PurePath):
        self.ensureLogged()
        uuidOf = self.getDirectoryUUID(directoryPath)
        if uuidOf:
            return uuidOf
        return None

    def isFile(self, filePath: PurePath):
        self.ensureLogged()
        if(self.getFileUniqueUUID(filePath)):
            return True
        else:
            return None

    def createFile(self, filePath: str, content, mode, offset: int = 0):
        self.ensureLogged()
        path = PurePath(filePath)
        directory = self.isDirectory(path.parent)
        if(directory == None):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        wholeContent = b''

        if(offset > 0):
            wholeFile = self.getFile(filePath)
            wholeContent = wholeFile.content[0:offset]
        readed = content.read()
        wholeContent = wholeContent + readed
        explicitModeSet = False
        if(type(mode) == tuple and len(mode) > 0):
            explicitModeSet = True
            mode = mode[0]
        fileExists = self.getFileMeta(path)
        if fileExists:
            if explicitModeSet:
                self.addFile(path.name, directory, mode, wholeContent, uniqueId = fileExists.uniqueid)
            else:
                self.addFile(path.name, directory, fileExists.flags, wholeContent, uniqueId = fileExists.uniqueid)
        else:
            self.addFile(path.name, directory, mode, wholeContent)
        return True, None

    def generateUuid(self):
        return (str(int(time.time()))[4:] + str(uuid4()).replace("-", "")[6:])[0:32]

    def addFile(self, name, directoryUUID, flags, content: bytes, uniqueId = None) -> str:
        if uniqueId == None:
            uuidNow = self.generateUuid()
        else:
            uuidNow = uniqueId
        if directoryUUID:
            directory = self.getDirectoryByUUID(directoryUUID)
            if not directory:
                raise Exception("Parent directory not found")
        self.client.sqlExec("""
            UPSERT INTO files(uniqueid, name, directory, creationdate, flags, content, filesize) VALUES(
                @uuid, @name, @directory, NOW(), @flags, @content, @filesize
            )
        """, {
            "uuid": uuidNow,
            "name": name,
            "directory": directoryUUID,
            "flags": flags,
            "content": content,
            "filesize": len(content)
        })
        return uuidNow

    def addDirectory(self, name, parentUUID, flags) -> str:
        uuidNow = self.generateUuid()
        if parentUUID:
            directory = self.getDirectoryByUUID(parentUUID)
            if not directory:
                raise Exception("Parent directory not found")
        self.client.sqlExec("""
            INSERT INTO directories(uniqueid, name, parent, creationdate, flags) VALUES(
                @uuid, @name, @parent, NOW(), @flags
            )
        """, {
            "uuid": uuidNow,
            "name": name,
            "parent": parentUUID,
            "flags": flags
        })
        return uuidNow

    def getDirectoryUUID(self, path: PurePath):
        self.ensureLogged()
        if(type(path) != PurePath):
            path = PurePath(path)
        root = self.client.sqlQuery("""SELECT uniqueid FROM directories WHERE name = @name""", {"name": "/"}, columnNameMode=COLUMN_NAME_MODE_FIELD)

        root = root[0]["uniqueid"]
        if(len(path.parents) == 0):
            return root

        parents = [x.name for x in path.parents]
        parents.reverse()
        parents = parents[1:]
        lastDirectory = None
        for parent in parents:
            if parent == "":
                parent = "/"
            directory = self.getDirectoryByName(root, parent)
            if directory:
                root = directory.uniqueid
                lastDirectory = directory
        lastDirectory = self.getDirectoryByName(root, path.name)
        if lastDirectory:
            return lastDirectory.uniqueid
        else:
            return None

    def getDirectoryByPath(self, path: PurePath):
        self.ensureLogged()
        uid = self.getDirectoryUUID(path)
        if uid:
            return self.getDirectoryByUUID(uid)
    
    def getDirectoryByName(self, parentDirectory, name):
        unique = self.client.sqlQuery("""SELECT * FROM directories WHERE parent = @uniqueid AND name = @name""", {"uniqueid": parentDirectory, "name": name}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        if unique:
            first = unique[0]
            return Directory(uniqueid=first["uniqueid"], name = first["name"], parent = first["parent"], creationdate = first["creationdate"], flags = first["flags"])
        else:
            return None

    def getDirectoryByUUID(self, uniqueid):
        unique = self.client.sqlQuery("""SELECT * FROM directories WHERE uniqueid = @uniqueid""", {"uniqueid": uniqueid}, columnNameMode=COLUMN_NAME_MODE_FIELD)
        if unique:
            first = unique[0]
            return Directory(uniqueid=first["uniqueid"], name = first["name"], parent = first["parent"], creationdate = first["creationdate"], flags = first["flags"])
        else:
            return None


    def createDirectory(self, filePath: str, mode = 16384):
        self.ensureLogged()
        if(not filePath.endswith("/")):
            filePath = filePath + "/"
        path = PurePath(filePath)
        parent = self.getDirectoryUUID(path.parent)
        if(not parent):
            return False, ErrorCode.DIRECTORY_NOT_EXISTS
        if(mode and type(mode) == tuple and len(mode) > 0):
            mode = mode[0]
        self.addDirectory(path.name, parent, mode)
        return True, None

    def appendToFile(self, filePath: str, content):
        self.ensureLogged()
        pass

    def updateFileDirectory(self, fileUUID: str, directoryUUID: str):
        self.client.sqlExec("""UPDATE files SET directory = @directory WHERE uniqueid = @uniqueid""", {"directory": directoryUUID, "uniqueid": fileUUID})
        return True

    def updateFileFilename(self, fileUUID: str, filename: str):
        self.client.sqlExec("""UPDATE files SET name = @filename WHERE uniqueid = @uniqueid""", {"filename": filename, "uniqueid": fileUUID})
        return True

    def updateFileFlags(self, fileUUID: str, flags: int):
        self.client.sqlExec("""UPDATE files SET flags = @flags WHERE uniqueid = @uniqueid""", {"flags": flags, "uniqueid": fileUUID})
        return True

    def updateDirectoryFlags(self, directoryUUID: str, flags: int):
        self.client.sqlExec("""UPDATE directories SET flags = @flags WHERE uniqueid = @uniqueid""", {"flags": flags, "uniqueid": directoryUUID})
        return True

        

    def updateDirectoryDirectory(self, uuidNow: str, directoryUUID: str):
        self.client.sqlExec("""UPDATE directories SET parent = @directory WHERE uniqueid = @uniqueid""", {"directory": directoryUUID, "uniqueid": uuidNow})
        return True

    def updateDirectoryName(self, uuidNow: str, directoryName: str):
        self.client.sqlExec("""UPDATE directories SET name = @name WHERE uniqueid = @uniqueid""", {"name": directoryName, "uniqueid": uuidNow})
        return True

    def move(self, filePath: str, newPath: str):
        self.ensureLogged()
        purePath1 = PurePath(filePath)
        purePath2 = PurePath(newPath)
        isSecondDir = self.isDirectory(purePath2)            
        fileMeta = self.getFileUniqueUUID(purePath1)
        if not fileMeta:
            isDirectory = self.isDirectory(purePath1)
            isSecondFile = self.getFileMeta(purePath2)
            if isDirectory and isSecondDir:
                self.updateDirectoryDirectory(isDirectory, isSecondDir)
                return True, None
            elif isDirectory and not isSecondFile:
                self.updateDirectoryName(isDirectory, purePath2.name)
                return True, None
            else:
                return False, ErrorCode.TARGET_DIRECTORY_IS_FILE





        if(fileMeta and isSecondDir):
            self.updateFileDirectory(fileMeta, isSecondDir)
            return True, None
        elif(fileMeta and not isSecondDir):
            isSecondDir = self.isDirectory(purePath2.parent)
            if not isSecondDir:
                return False, ErrorCode.TARGET_FILE_IS_DIRECTORY
            self.updateFileDirectory(fileMeta, isSecondDir)
            self.updateFileFilename(fileMeta, purePath2.name)
            return True, None
        else:
            return False, ErrorCode.SOURCE_FILE_NOT_EXISTS

    def overrideFile(self, filePath: str, content):
        self.ensureLogged()
        pass


