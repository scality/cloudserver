var gulp = require('gulp'),
    jshint = require('gulp-jshint'),
    shell = require('gulp-shell');

/* lint all the things! */
gulp.task('lint', function() {
  return gulp.src('./lib/*.js')
    .pipe(jshint())
    .pipe(jshint.reporter('default'))
    .pipe(jshint.reporter('fail'));
});

/* install pre-commit hook */
gulp.task('precommit-hook', shell.task([
  'cp .pre-commit .git/hooks/pre-commit 2>/dev/null || :'
]));

gulp.task('default', ['lint']);
