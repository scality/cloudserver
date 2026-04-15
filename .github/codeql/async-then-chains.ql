/**
 * @name Promise .then() usage (async migration)
 * @description These calls use .then() instead of async/await. They should be refactored to use async/await.
 * @kind problem
 * @problem.severity recommendation
 * @id js/promise-then-usage
 * @tags maintainability
 *       async-migration
 */

import javascript

from MethodCallExpr m
where
  m.getMethodName() = "then" and
  // Exclude test files and node_modules
  not m.getFile().getAbsolutePath().matches("%/tests/%") and
  not m.getFile().getAbsolutePath().matches("%/node_modules/%")
select m, "This call uses .then(). Refactor to async/await."
