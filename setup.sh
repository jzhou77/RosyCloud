#!/usr/bin/env bash
BASE_VERSION="2.5.0"
INSTALL_MODULE="install -r"
MKDIR_IF_NOT_EXIST="mkdir -vp"
PYTHON=python
REQUIREMENTS=requirements.txt
# source file directory
SRC_HOME=$HOME/bin/rosycloud_src
# syncing directory
ROSYCLOUD_HOME=$HOME/RosyCloud
# system wide configure
CONFIGS=$HOME/.rosycloud

HOME_BIN=$HOME/bin
DEST_CMD=$HOME_BIN/rosycloud
VENV_HOME=$HOME/.virtualenv
SCRIPT_TMPL=templates/rosycloud.tmpl

PYTHON_MODULE_INSTALLER=$VENV_HOME/bin/pip

# python release should be later than 2.5
echo $PYTHON
version=`$PYTHON -V 2>&1 | sed -n 's/^Python \(\([0-9]\+\.\?\)\{3\}\)/\1/p'`

if [ "$version" \< $BASE_VERSION ]; then
    echo -n "Python version "
    echo -n $version
    echo " is too old."
    echo -n "To run this app, python release should be late than "
    echo -n $BASE_VERSION
    echo "."

    exit 255
fi

# install virtual env first
$PYTHON virtualenv.py $VENV_HOME
source $VENV_HOME/bin/activate

# install dependencies
$PYTHON_MODULE_INSTALLER $INSTALL_MODULE $REQUIREMENTS 2>/dev/null

# install rosycloud source
$MKDIR_IF_NOT_EXIST $SRC_HOME
$MKDIR_IF_NOT_EXIST $ROSYCLOUD_HOME
# copy source files
cp -rv src/* $SRC_HOME
# copy config files
cp -v  templates/exclude.tmpl $ROSYCLOUD_HOME/
cp -v  templates/config.tmpl  $SRC_HOME/
# generate executable from template
sed -e "s|VIRTUALENV_HOME|$VENV_HOME|g" -e "s|SRC_HOME|$SRC_HOME|g" $SCRIPT_TMPL > $DEST_CMD

chmod u+x $DEST_CMD

# initialize system configure directory
mkdir -pv $CONFIGS
mkdir -pv $CONFIGS/cache
mkdir -pv $CONFIGS/dir
mkdir -pv $CONFIGS/snapshots
# temporary directory
mkdir -pv $CONFIGS/tmp

# installation done
echo
echo "Installation DONE!"
