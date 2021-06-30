# Running Azurite

(Azurite)[https://github.com/azure/azurite] is the blob server mock developed by Azure itself.

## Running with docker

The following command will run Azurite locally on your machine with the proper credentials
to run Cloudserver's test.

```shell
docker run --rm --env AZURITE_ACCOUNTS="fakeaccountname:Fake00Key001;fakeaccountname2:Fake00Key002" -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
```
