# S3

[![badge][badge]](https://ci.ironmann.io/gh/scality/S3/tree/master)

Kick ass S3 server clone

## Installation

```shell
npm install --save scality/S3
```

## Run it

```shell
npm start
```

## Testing

You can run the unit tests with the following command:

```shell
npm test
```

You can run the linter with:

```shell
npm run lint
```

You can run local functional tests with:

```shell
npm start &
npm run ft_test
```

## s3cmd versions

If using s3cmd as a client to S3 be aware that v4 signature format
is buggy in s3cmd versions < 1.6.1.

[badge]: https://ci.ironmann.io/gh/scality/S3.svg?style=shield&circle-token=83d0efd99242ca1bc15703b02d2beb72a77aadf2
