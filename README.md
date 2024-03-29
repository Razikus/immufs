# immufs

Immutable verifable filesystem on top of libfuse and immudb

## showcase
[![asciicast](https://asciinema.org/a/519412.svg)](https://asciinema.org/a/519412)

## Features
- [x] mkdir
- [x] chmod
- [x] mv
- [x] create and retrieve files up to 4mb
- [x] remove files
- [x] remove directories
- [x] retrieve files less than 4mb
- [x] file size
- [ ] retrieve files more than 4mb (immudb max response, need to move to streaming)
- [ ] fchmod
- [ ] chown
- [ ] fchown
- [ ] last modified and utime
- [ ] show how much space left (ImmuStatVFS)
- [ ] lock
- [ ] links
- [ ] symlinks
- [ ] access
- [ ] mknod
- [ ] readlink

## Use cases
- immutable documents storage
- ransomware-resistant if storage is remote
- with addition of public/private key - easy to implement encrypted immutable storage
- instead of storing values we can make a proxy between original system and immudb to just watch access/calculate changes/log all
- immutable backups in future 

## Requirements

### Python
```pip3 install -r requirements.txt```

### Debian apt
```apt install -y libfuse-dev```


## Auditing
To enable full potential of immufs and immudb you should always consider external auditor that will constantly / on scheduled time verify your database.

