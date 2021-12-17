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

* Checkout the relevant branch. In this example,
  we are working on development/8.3, and we want to release version `8.3.0`.

```sh
git checkout development/8.3
```

* Tag the branch with the release version. In this example, `8.3.0`

```sh
git tag -a 8.3.0
# The message should be 'v<version>'
v8.3.0
```

* Push the tags to GitHub.

```sh
git push --tags
```

* With the following parameters, [force a build here](https://eve.devsca.com/github/scality/cloudserver/#/builders/3/force/force)

    * Branch Name: The one used for the tag earlier. In this example 'development/8.3'
    * Override Stage: 'release'
    * Extra properties:
      * name: `'tag'`, value: `[release version]`, in this example`'8.3.0'`

* Once the docker image is present on [registry.scality.com](registry.scality.com),
  update CloudServers' `package.json`
  by bumping it to the relevant next version in a new PR.
  In this case, `8.3.1` .

```js
{
  "name": "@zenko/cloudserver",
  "version": "8.3.1", <--- Here
  [...]
}
```

* Finally, once your PR has been reviewed, release the release version on Jira,
  set up the next version, and approve your PR.

  * Go to the [CloudServer release page](https://scality.atlassian.net/projects/CLDSRV?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page)
  * Create a new version if necessary
    * Name: `[next version]`, in this example `8.3.1`
    * Start Date: `[date of the release]`
  * Click `...` and select `Release` on the release version
  * Return to the release ticket,
    change the fix version of the ticket to the new version
  * Return to your PR and type `/approve`
