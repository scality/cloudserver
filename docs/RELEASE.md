# Cloudserver Release Plan

## Docker Image Generation

Docker images are hosted on [ghcri.io](https://github.com/orgs/scality/packages).
CloudServer has a few images there:

* Cloudserver container image: ghcr.io/scality/cloudserver
* Dashboard oras image: ghcr.io/scality/cloudserver/cloudserver-dashboards

With every CI build, the CI will push images, tagging the
content with the developer branch's short SHA-1 commit hash.
This allows those images to be used by developers, CI builds,
build chain and so on.

Tagged versions of cloudserver will be stored in the production namespace.

## How to Pull Docker Images

```sh
docker pull ghcr.io/scality/cloudserver:<commit hash>
docker pull ghcr.io/scality/cloudserver:<tag>
```

## Release Process

To release a production image:

* Create a PR to bump the package version : update Cloudserver's `package.json` by bumping it to the relevant next version in a new PR. Per example if the last released version was `8.4.7`, the next version would be `8.4.8`.

  ```js
  {
    "name": "cloudserver",
    "version": "8.4.8", <--- Here
    [...]
  }
  ```

* Review & merge the PR

* Trigger the release workflow on GitHub

  * Go to the [**Actions** tab on GitHub](https://github.com/scality/cloudserver/actions)
  * Select the `release` workflow from the list
  * Click on **Run workflow** (manual dispatch)
  * Enter the new tag (e.g., `8.4.8`) in the input field
  * Start the workflow

  This workflow will create the tag and push the Docker images.

  This should be done as soon as the PR is merged, so that the tag is put on the "version bump" commit.

* Release the release version on Jira

  * Go to the [CloudServer release page](https://scality.atlassian.net/projects/CLDSRV?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page)
  * Create a next version
    * Name: `[next version]`, in this example `8.4.9`
  * Click `...` and select `Release` on the recently released version (`8.4.8`)
  * Fill in the field to move incomplete version to the next one
