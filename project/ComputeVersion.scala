import scala.sys.process.*

/** It computes the project version starting from git repository information
 *
 *  The project version is not stored anywhere, but is instead extracted from the current branch and tags.
 *  The version is then used to publish the docker image produced as output artifact in our container registry.
 *  The versioning system must be handled in the following way:
 *
 *  - new issues are merged always into the master branch.
 *  - every time we want to create a new version we must:
 *   1. create a new release branch from master with the major.minor version number (e.g. release/1.2)
 *   2. create a tag for that version (e.g. v1.2.0)
 *  - we will _always_ deploy images generated from tags (stable version)
 *  - when we need to make an hotfix on an older version, we will push the issue on the relative release branch, and create a new tag (e.g. if we want to fix
 *   an error on version 1.2.0 we will merge in on release branch release/1.2 and then create a new tag v1.2.1)
 *
 *  Creating images from feature/hotfix branches is discouraged, but in case it is needed they will behave exactly like the master branch (the SNAPSHOT version
 *  always contain the commit SHA and the log message to simplify discriminating them).
 */
object ComputeVersion {

  lazy val version: String = ("chmod +x scripts/get-version.sh" #&& "scripts/get-version.sh").lineStream_!.head
}
