# Hopsworks Scripts
<!-- Copyright (c) 2023, 2023, Hopsworks and/or its affiliates. -->
## config_git
From the repository root, run `./hopsworks_scripts/config_git`. This will do the following:
- Install a pre-commit hook that makes sure that each commit updates copyright notices correctly. If you attempt to create a commit that changes a file without a correct and up-to-date copyright notice for Hopsworks, git will refuse to create the commit with a detailed error message.
- Set the conflict style to `diff3`. This means that merge conflicts will also helpfully display the common parent of the conflict.
- Check whether git is configured to identify you as a Hopsworks employee, and if not, give instructions for how to fix this.

