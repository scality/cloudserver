/**
 * @name Callback-style function (async migration)
 * @description These functions use callback parameters. They should be refactored to use async/await.
 * @kind problem
 * @problem.severity recommendation
 * @id js/callback-style-function
 * @tags maintainability
 *       async-migration
 */

import javascript

from Function f, Parameter p
where
  p = f.getParameter(f.getNumParameter() - 1) and
  p.getName().regexpMatch("(?i)^(cb|callback|next|done)$") and
  not f.isAsync() and
  // Exclude test files and node_modules
  not f.getFile().getAbsolutePath().matches("%/tests/%") and
  not f.getFile().getAbsolutePath().matches("%/node_modules/%")
select f, "This function uses a callback parameter ('" + p.getName() + "'). Refactor to async/await."
