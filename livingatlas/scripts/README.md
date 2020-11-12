# Scripts for running pipelines

The following scripts all use the `set-env.sh` file to configure the environment.
Please review these properties to switch filesystems (e.g. EFS, HDFS, Local FS) and between cluster and non-cluster.

The scripts in this directory are temporary and will be superceded by the work in https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/88 type:

```
./la-pipelines --help
```

for details.

## la-pipelines CLI Autocompletion

If you want to use `la-pipelines` with TAB autocompletion in your local development environment and `bash` shell, you can run:
```
source la-pipelines-bash-completion.sh
```
or copy to:
```
/etc/bash_completion.d/la-pipelines.sh
```
This is installed by the debian package and should work out-of-the-box in production.