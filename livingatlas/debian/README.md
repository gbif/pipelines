## Intro

## Prerequisites

```
sudo apt install debhelper
sudo apt install devscripts
```

## Build

You need to create a link in the repository root like:
```
ln -s livingatlas/debian debian
```
because many build tools we use look for the debian directory in the root of the project.

After that, you can generate a non-signed debian package via:

```bash
debuild -us -uc -b
```
in the root of this repository. This will generate the deb file in the parent directory of this repository.

If you have build the jars in another tasks (for instance in another jenkins build), you can skip it with:
```
debuild -us -uc -b  --build-profiles=nobuildjar
```

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
