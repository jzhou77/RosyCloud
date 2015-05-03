# How to Install:
type `./setup.sh` to run install script.

By default, the python file will be copied into directory "$HOME/bin/rosycloud_src/".
The default RosyCloud's backup source is local directory "$HOME/RosyCloud/" and
destination can be configured (see `How to configure' for details), i.e.,
contents in this directory will be synchronized and backed-up.

A Python virtual environment will be installed accompanied with RosyCloud, so be not
afraid of the installation will pollute your default Python settings.
However, we require the original system Python is later than version *2.5.0*.

# How to configure:
After `setup.sh' finishes, you will find two configuration templates under 
"$HOME/bin/rosycloud_src/" and "$HOME/RosyCloud/" respectively:
config.tmpl - specifies the system wide configuration template
and 
exclude.tmpl - specifies patterns for excluding files for RosyCloud syncs

We detail the configuration entries in config.tmpl while you can leave most of them
unchanged

* *SRC_DIR* the directory needs sync-ed and backed-up
* *SYS_DIR* the destination to store system cache and temporary file
* *GPG_HOME* and *GPG_FILE* are utilities directory

we highly recommend you take default values of these fields.

* *EXCLUDE_FILE* indicates which file to refer to to exclude unconcerned files when
backup and sync.
* *CLOUDS* specifies which cloud(s) to be considered as backends.
Currently, RosyCloud inherently supports local disk, Aliyun OSS, and Windows Azure.

For multiple backends, all the storage services should be specified and seperated by
`:'. Each storage service has a service specific configuration file corresponding to it.
All the recognizable cloud service and their configuration files (the naming convention
is cloud_name.conf) are:
    +-----------+----------------+---------------+
    |   Cloud   |   Configure    |  Description  |
    +-----------+----------------+---------------+
    |   local   |   local.conf   | local storage |
    |    oss    |    oss.conf    |  Aliyun OSS   |
    |   azure   |   azure.conf   | Windows Azure |
    |googledrive|googledrive.conf| Google Drive  |
    +-----------+----------------+---------------+

Templates of the configuration files are all in the path `src/'.
You can add your own cloud storage as well. In that case, class *BackupFileSystem* should
be extended from `src/fs/bakfilesystem.py' and add your cloud name to
(driver file, driver class name) mapping in the *cloud_map* in function *init_clouds* from
file `src/rosycloud.py'.

* *INTERVAL* is synchronization time interval.

After that, rename config.tmpl as ".config". Then, modify exclude.tmpl and save as ".exclude",
whose file name should be consistent with EXCLUDE_FILE in the .config file.

# How to run:
RosyCloud provide several sub-commands, they are
    +--------+----------------------------------+--------------------------------------+
    |command |           Description            |             command usage            |
    +--------+----------------------------------+--------------------------------------+
    |versions|list versioned files              | versions path                        |
    |  tag   |tag a versioned path              | tag tagname path                     |
    | xtract |extract specific file or directory| xtract cloud version path            |
    | start  |start monitoring the directory    | start                                |
    +--------+----------------------------------+--------------------------------------+

The `share' and `grant' tools, alledged in our paper
`Versioned File Backup and Synchronization in Storage Cloud' published on CCGrid 2013,
are not fully tested and will be included in next release.

* run command `$HOME/bin/rosycloud *run* subcommand' to start sync and monitor changes
* run command `$HOME/bin/rosycloud *stop*' to stop the daemon.

# How to uninstall:
Script `uninstall.sh' will remove all files related to RosyCloud.
The installation will not pollute your working environemnt.
After uninstallation, your environment will be as clean as it previously was.

Voila.
Happy sync-ing!
