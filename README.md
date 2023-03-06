# LemonDB Recovered Files

# Honor Code Pledge
If there is similar course materials assigned in the future, it is the responsibility of JI students not to copy or modify these codes, or other files because it is against the Honor Code. The owner of this repository doesn't take any commitment for other's faults.

## Introduction

- ./src
   Contains the source code for an old version of lemnonDB.
   The code was recovered from crash site. As far as we know the
   original developer used CMake as their building system.

- ./bin
   Contains the lastest binary that survived the crash.

- ./db
   Contains sample database files.

- ./sample
   Sample inputs and outputs

## Developer Quick Start

See INSTALL.md for instructions on building from source.

`ClangFormat` and `EditorConfig` are used to format codes.

Hint on using `ClangFormat`:
`find . -name "*.cpp" -o -name "*.h" | sed 's| |\\ |g' | xargs clang-format -i`

And make sure your code editor has `EditorConfig` support.

[![Build Status](https://focs.ji.sjtu.edu.cn:2222/api/badges/ECE482-22/p2team-02/status.svg)](https://focs.ji.sjtu.edu.cn:2222/ECE482-22/p2team-02)

## Copyright

Lemonion Inc. 2018

