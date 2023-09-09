#!/usr/bin/env bash

set -o errexit # exit when a command fails
# set -o nounset # exit when a script tries to use undeclared variables
set -o xtrace # trace what gets executed

# This script calculates the current version based on the current branch and on the existing tags:
# - For tags (in the form v1.0.2) the version returned is always a stable one; in the previous example it would return 1.0.2.
# - For release branches (in the form relelase/3.1) it will return a SNAPSHOT version with the patch number increased by 1. The current patch number
#   is extracted from the highest tag with the same major.minor version as the release branch. The relelase branches will be updated only to backport
#   small functionalities or hotfixes, so they will just increase the patch number. In this example it could return 3.1.1-SNAPSHOT.bb67482.381-test.
# - For all other branches, it will find the highest tag available and return a SNAPSHOT version with patch 0 and the minor increased by 1.
#   So in case the highest tag is 1.2.4 it would return 1.3.0-SNAPSHOT.bb67482.381-test.
# Since artifacts are built from the master branch, relelase branches, and tags, we should not worry about feature/hotfix branches, but it any case
# they will behave exactly like the master branch.

# calculate the snapshot suffix by adding an extract from the commit message and the commit SHA
compute_snapshot_description () {
    # extract the last message log, removes non-ascii characters, replaces non-alphanumeric characters with dashes, and take teh first 20 characters lowercased
    message=$(git log -1 --pretty=%B | head -n 1 | iconv -t ascii//TRANSLIT | sed -r s/[^a-zA-Z0-9]+/-/g | sed -r s/^-+\|-+$//g | cut -c1-20 | tr A-Z a-z)
    # extract the last commit SHA in its shortened version
    sha=$(git rev-parse --short HEAD)
    echo "-SNAPSHOT.$sha.$message"
}

# gets the current git location by trying first with the current branch name. If that fails, it extracts the current tag name.
# if that fails as well, it simply gets the shortened SHA of the latest commit
get_current_branch_or_tag_name () {
  if [ -z ${CI_COMMIT_REF_NAME+x} ]; then
    branch=$(git branch --show-current)
    if [[ $branch ]]; then
        echo $branch
    else
        echo $(git describe --tags --exact-match || git rev-parse --short HEAD)
    fi
  else
    echo $CI_COMMIT_REF_NAME
  fi
}

# gets the current branch or tag name
branch_name=$(get_current_branch_or_tag_name)

# these regexs are used to determine if the current location is a release branch (e.g. 'release/2.1') or a tag (e.g. 'v1.2.1')
release_branch_regex='release/([0-9]+)\.([0-9]+)'
tag_regex='v([0-9]+)\.([0-9]+)\.([0-9]+)'

# if the current location matches the tag regex
if [[ $branch_name =~ $tag_regex ]]; then
    # extract the three variables (major, minor, patch) and return that version
    echo "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
# else if the location matches the relelase branch regex
elif [[ $branch_name =~ $release_branch_regex ]]; then
    # extract the major and minor variables and calculate the snapshot suffix
    major=${BASH_REMATCH[1]}
    minor=${BASH_REMATCH[2]}
    snapshot=$(compute_snapshot_description)
    # get the highest tag that matches the major and minor version
    last_tag=$(git tag --list v$major.$minor* --sort=-v:refname | head -n 1)
    # if no tag was found with that major and minor version
    if [ -z $last_tag ]; then
        # just return a SNAPSHOT version with that major.minor and with patch 1
        echo "$major.$minor.1$snapshot"
    else
        # otherwise extract the variables from the highest tag found
        if [[ $last_tag =~ $tag_regex ]]; then
            patch=${BASH_REMATCH[3]}
            # increase teh patch number by 1
            new_patch=$(($patch+1))
            # and return that snapshot
            echo "$major.$minor.$new_patch$snapshot"
        fi
    fi
# otherwise we are not in a tag location and neither on a release branch, so we will assuem this is the master branch.
# please note that this is where we will end up also for feature/hotfix branches, but usually we will not generate any artifacts from them.
# if, for testing reason, we need to create an artifact from a feature/hotfix branch, it will have a version as if it was taken from the master branch.
else
    # calculate the snapshot suffix
    snapshot=$(compute_snapshot_description)
    # find the highest tag
    last_tag=$(git tag --list --sort=-v:refname | head -n 1)
    # if there is no tag
    if [ -z $last_tag ]; then
        # return the 0.0.0-SNAPSHOT version
        echo "0.0.0$snapshot"
    else
        # otherwise extract the variables from the highest tag found
        if [[ $last_tag =~ $tag_regex ]]; then
            major=${BASH_REMATCH[1]}
            minor=${BASH_REMATCH[2]}
            # increase the minor number by 1
            new_minor=$(($minor+1))
            # and return that snapshot
            echo "$major.$new_minor.0$snapshot"
        fi
    fi
fi