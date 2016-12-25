#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

/*
Crude test of this program:

for size in 0 1 2 7 8 9 15 16 17 18 29;
do ./knownfile --size=$size toto.$size ;
   for offset in 1 2 3 4 5 6 7 8 ;
   do
       if [ $offset -lt $size ] ;
       then dd if=toto.$size of=toto.$size.$offset bs=1 skip=$offset \
            2>/dev/null;
            if ./knownfile --size=$(($size-$offset)) toto.$size.$offset \
                --check --offset=$offset;
            then rm -f toto.$size.$offset; else echo Oops toto.$s.$offset ;
            fi ;
       fi;
   done;
done
*/

int usage(const char *reason)
{
    fprintf(stderr,
"Usage: knownfile [-h] [--check] [--size SIZE] [--offset OFFSET] [--quiet] \
filename\n"
"\n"
"Create a file of specified length / Check extract\n"
"\n"
"positional arguments:\n"
"filename                    file to use\n"
"\n"
"optional arguments:\n"
"-h, --help                  show this help message and exit\n"
"--check, -c                 check mode\n"
"--size SIZE, -s SIZE        size of the file\n"
"--offset OFFSET, -o OFFSET  offset of the file\n"
"--quiet, -q                 quiet mode\n"
        );
    if (reason) {
        fprintf(stderr, "ERR: %s\n", reason);
    }
    return 1;
}

static int quietmode = 0;
void quietprint(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    if (!quietmode) {
        vfprintf(stderr, fmt, args);
        fprintf(stderr, "\n");
    }
    va_end(args);
}

static inline uint8_t offset2val(uint64_t a)
{
    a = (a ^ 61) ^ (a >> 16);
    a = a + (a << 3);
    a = a ^ (a >> 4);
    a = a * 0x27d4eb2d;
    a = a ^ (a >> 15);
    return a % 256;
}

void makevals(uint8_t *vals, uint64_t offset, uint64_t size)
{
    int i;
    for (i = 0; i < size; i++) {
        vals[i] = offset2val(offset + i);
    }
}

static uint8_t values[120 * 1024];
int kf_create(const char *filename, uint64_t size)
{
    FILE *f = NULL;
    uint64_t cur, towrite;
    int ret = 1;
    f = fopen(filename, "wb+");
    if (!f) {
        quietprint("Cannot open '%s' for writing", filename);
        goto end;
    }
    for (cur = 0; cur < size; cur += towrite) {
        towrite = size - cur;
        if (towrite > 120 * 1024) {
            towrite = 120 * 1024;
        }
        makevals(values, cur, towrite);
        if (fwrite(values, towrite, 1, f) != 1) {
            quietprint("Failed writing to '%s' at offset %lu", filename, cur);
            goto end;
        }
    }
    ret = 0;
end:
    if (f) {
        fclose(f);
        f = NULL;
    }
    return ret;
}

int kf_check(const char *filename, uint64_t size, uint64_t offset)
{
    int fd = -1;
    uint8_t *ptr = MAP_FAILED;
    uint64_t cur;
    int ret = 1;
    struct stat sb;

    fd = open(filename, O_RDONLY);
    quietprint("Checking %s / size:%lu / offset:%lu", filename, size, offset);
    if (fd == -1) {
        quietprint("open of \"%s\" for reading failed", filename);
        goto end;
    }
    if (fstat(fd, &sb) == -1) {
        quietprint("fstat failed on \"%s\"", filename);
        goto end;
    }
    if (sb.st_size != size) {
        quietprint("size check failed on \"%s\". Expected %lu, got %lu",
            filename, size, sb.st_size);
        goto end;
    }
    ptr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        quietprint("mmap failed on \"%s\"", filename);
        goto end;
    }
    for (cur = 0; cur < sb.st_size; cur++) {
        if (ptr[cur] != offset2val(offset + cur)) {
            quietprint("Content check failed at offset %lu in  \"%s\", offset \
                %lu in knownfile", cur, filename, offset + cur);
            goto end;
        }
    }
    ret = 0;
    quietprint("Success");
end:
    if (ptr != MAP_FAILED) {
        munmap(ptr, sb.st_size);
    }
    if (fd >= 0) {
        close(fd);
    }
    return ret;
}

int main(int argc, char **argv)
{
    int c;
    int checkmode = 0;
    uint64_t size, offset;
    const char * filename;
    int size_set = 0;
    int offset_set = 0;

    while (1) {
        int this_option_optind = optind ? optind : 1;
        int option_index = 0;
        static struct option long_options[] = {
            {"help", no_argument, 0, 'h'},
            {"quiet", no_argument, 0, 'q'},
            {"check", no_argument, 0, 'c'},
            {"size", required_argument, 0, 's'},
            {"offset", required_argument, 0, 'o'},
            {0, 0, 0, 0}
        };
        c = getopt_long(argc, argv, "h?qcs:o:", long_options, & option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
            case 0:
                return usage(NULL);
                break;
            case 'c':
                checkmode = 1;
                break;
            case 's':
                size = strtoll(optarg, NULL, 10);
                size_set = 1;
                break;
            case 'o':
                offset = strtoll(optarg, NULL, 10);
                offset_set = 1;
                break;
            case 'q':
                quietmode = 1;
                break;
            case 'h':
            case '?':
                return usage(NULL);
                break;
        }
    }
    if (optind != argc - 1) {
        return usage("Wrong number of arguments");
    }
    filename = argv[optind];
    if (!size_set) {
        return usage("Missing size\n");
    }
    if (checkmode) {
        if (!offset_set) {
            offset = 0;
        }
        return kf_check(filename, size, offset);
    } else {
        return kf_create(filename, size);
    }
    return 0;
}
