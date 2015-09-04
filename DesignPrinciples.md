## Design Principles:

1. Why is more important than how, each module must have a design and rationale documentation - Design.md directly in the source under version control
 * Folder get a Design.md
 * If a file is complex or important enough, it can have its own design file: “bucket.js” gets a bucket.js.Design.md
2. DRY "Do not repeat yourself”: duplication of code is the root of all evil
3. Abstraction principle: Never call system functions & external libraries directly - wrap stuff for loosely coupling
4. Write code for maintainer: it takes days to write code but years to support
5. Simple is better: don’t over engineer, don’t implement features that are not needed yet

## Coding principles:

1. Usage of IronMan config and log libraries is mandatory 
2. Adhere to our coding style - automated lint
3. No callback hell - check errors and return early in callbacks
4. Use Nan for external C/C++ modules - no direct v8
5.Tests are more important than the code itself - pertinence is key as well as metrics
6. Develop in branches, do pull requests, mandatory code review including reviewing the tests and Design.md

