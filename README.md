# immufs

Immutable verifable filesystem on top of libfuse and immudb

## showcase
[![asciicast](https://asciinema.org/a/519412.svg)](https://asciinema.org/a/519412)

## Features
[x] mkdir
[x] chmod
[x] mv
[x] create and retrieve files up to 4mb
[x] remove files
[x] remove directories
[x] retrieve files less than 4mb
[x] file size
[] retrieve files more than 4mb (immudb max response, need to move to streaming)
[] fchmod
[] chown
[] fchown
[] last modified
[] show how much space left


## Requirements

### Python
```pip3 install -r requirements.txt```

### Debian apt
```apt install -y libfuse-dev```


## Auditing
To enable full potential of immufs and immudb you should always consider external auditor that will constantly / on scheduled routing verify your database.

