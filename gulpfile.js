var gulp = require('gulp'),
    guppy = require('git-guppy')(gulp),
    jshint = require('gulp-jshint'),
    stylish = require('jshint-stylish'),
    gulpFilter = require('gulp-filter');

/* lint all the things! */
gulp.task('lint', function() {
  return gulp.src('./lib/*.js')
    .pipe(jshint())
    .pipe(jshint.reporter(stylish));
});


/* precommit hook for linting */
gulp.task('pre-commit', function() {
  return guppy.stream('pre-commit')
  .pipe(gulpFilter(['*.js']))
  .pipe(jshint())
  .pipe(jshint.reporter(stylish))
  .pipe(jshint.reporter('fail'));
});

gulp.task('default', ['lint']);
