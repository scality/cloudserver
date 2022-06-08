# Cloudserver Release Plan

## Docker Image Generation

Docker images are hosted on [registry.scality.com](registry.scality.com).
CloudServer has two namespaces there:

* Production Namespace: registry.scality.com/cloudserver
* Dev Namespace: registry.scality.com/cloudserver-dev

With every CI build, the CI will push images, tagging the
content with the developer branch's short SHA-1 commit hash.
This allows those images to be used by developers, CI builds,
build chain and so on.

Tagged versions of cloudserver will be stored in the production namespace.

## How to Pull Docker Images

```sh
docker pull registry.scality.com/cloudserver-dev/cloudserver:<commit hash>
docker pull registry.scality.com/cloudserver/cloudserver:<tag>
```

## Release Process

To release a production image:

* Create a PR to bump the package version
  Update Cloudserver's `package.json` by bumping it to the relevant next
  version in a new PR. Per example if the last released version was
  `8.4.7`, the next version would be `8.4.8`.

```js
{
  "name": "cloudserver",
  "version": "8.4.8", <--- Here
  [...]
}
```

* Review & merge the PR

* Create the release on GitHub
  
  * Go the Release tab (https://github.com/scality/cloudserver/releases);
  * Click on the `Draft new release button`;
  * In the `tag` field, type the name of the release (`8.4.8`), and confirm
    to create the tag on publish;
  * Click on `Generate release notes` button to field the fields;
  * Rename the release to `Release x.y.z` (e.g. `Release 8.4.8` in this case);
  * Click to `Publish the release` to create the GitHub release and git tag

  Notes:
  * the Git tag will be created automatically.
  * this should be done as soon as the PR is merged, so that the tag
    is put on the "version bump" commit.

* With the following parameters, [force a build here](https://eve.devsca.com/github/scality/cloudserver/#/builders/3/force/force)

  * Branch Name: The one used for the tag earlier. In this example `development/8.4`
  * Override Stage: 'release'
  * Extra properties:
    * name: `'tag'`, value: `[release version]`, in this example`'8.4.8'`

* Release the release version on Jira

  * Go to the [CloudServer release page](https://scality.atlassian.net/projects/CLDSRV?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page)
  * Create a next version
    * Name: `[next version]`, in this example `8.4.9`
  * Click `...` and select `Release` on the recently released version (`8.4.8`)
  * Fill in the field to move incomplete version to the next one
