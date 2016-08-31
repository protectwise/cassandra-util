#!/bin/bash
if [ ! -d .git ]; then
	echo "This script must be run from the working copy root"
	exit 1
fi
git subtree pull --prefix=tools/ccm https://github.com/pcmanus/ccm.git master --squash
