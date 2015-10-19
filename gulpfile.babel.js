import gulp from 'gulp';
import jshint from 'gulp-jshint';
import shell from 'gulp-shell';

/* lint all the things! */
gulp.task('lint', function lintIt() {
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
