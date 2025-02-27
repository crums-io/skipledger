## About this Directory

For lack of a better place to put them, native-image configuration files are kept here.

Right now these are used directly from command-line tools, not thru maven.

These are initially generated via something like

```
$ java -agentlib:native-image-agent=config-output-dir=conf-dir -jar mrsl-0.5.1-SNAPSHOT ../../sandbox/chinook/chinook-800-830-r.mrsl report 150 -s sample.pdf
```

using the [LNIK 22.2](https://bell-sw.com/announcements/2022/08/05/liberica-native-image-kit-22-2-0-and-21-3-3-builds-are-out/)
(a GraalVM derivative). Then, without editing anything

```
$ native-image -Djava.awt.headless=false -H:ReflectionConfigurationFiles=conf-dir/reflect-config.json -H:ResourceConfigurationFiles=conf-dir/resource-config.json -jar mrsl-0.5.1-SNAPSHOT-jar-with-dependencies.jar
```
This produces a 56MB image. A bit big, but not a deal breaker. This works for *that* morsel file. To make it work
with every morsel file, we'll need to set additional resources (see below).

### TODO

The [resource-config.json](./resource-config.json) file needs editing (why I'm leaving this note, before moving on):
it must include *all possible resource types* a report template (its DSL) might reference, not just the ones
used in the example morsel file. Include these by manually listing *everything* in the `com.lowagie.text.pdf.fonts` package.
(The OpenPDF resources are not organized under their own directory, but those are the only resources I can find
that are not picked automatically by `java -agentlib:native-image-agent=..`.)

Also consider adding `openpdf-fonts-extra` as a dependency (and then include its font resources). Since the image
is already big, the size of these extra resources (2.3MB) shouldn't be a consideration.

