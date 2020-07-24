## Intro

## Prerequisites

```
sudo apt install debhelper
sudo apt install devscripts
```

## Build

You can generate a non-signed debian package via:

```bash
debuild -us -uc -b
```
in the parent of this directory. This will generate the deb file in the parent directory of this repository.

You can increase changelog version and comments with `dch` utility, like with `dch -i` that increase minor version.

Add to your .gitignore:
```
*/*.debhelper
*/*.debhelper.log
*.buildinfo
*.substvars
debian/files
debian/la-pipelines
```

## Testing

You can test the generated package without install it with `piuparts` like:

```bash
sudo piuparts -D ubuntu -d xenial -d bionic ../la-pipelines_1.0-SNAPSHOT_all.deb
```
in this way you can also test it in different releases.

Read `/usr/share/doc/piuparts/README.*` for more usage samples.

