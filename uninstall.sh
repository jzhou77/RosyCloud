#!/usr/bin/env bash
# source file directory
SRC_HOME=$HOME/bin/rosycloud_src

HOME_BIN=$HOME/bin
DEST_CMD=$HOME_BIN/rosycloud
VENV_HOME=$HOME/.virtualenv
CONFIGS=$HOME/.rosycloud

rm -rvf $DEST_CMD
rm -rvf $SRC_HOME
rm -rvf $VENV_HOME
rm -rvf $CONFIGS
