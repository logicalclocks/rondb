This directory has a number of "vendored" dependencies:
- drogon/drogon-<VERSION>/
- drogon/trantor-<VERSION>/
- simdjson/simdjson-<VERSION>/

Every change to these dependencies is captured in ./*.patch, and these patches
need to be applied after an upgrade. When you change a vendored source file,
add or update its patch file in the same commit; a change without a patch file
is lost in the next upgrade. In more detail, to upgrade a vendored dependency:

1) In a separate commit, add the new files. For example, add
   drogon/drogon-<NEWVERSION>/ without deleting drogon/drogon-<OLDVERSION>/
2) In a separate commit, apply changes:
   a) Edit the version number in the parent CMakeLists.txt, i.e.
      drogon/CMakeLists.txt or simdjson/CMakeLists.txt.
   b) Edit ./*.patch to apply to the new version number, and other necessary
      changes.
   c) Apply patch files using `git apply`
3) In a separate commit, delete the old version, e.g. drogon/drogon-<OLDVERSION>/
