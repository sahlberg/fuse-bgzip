/* -*-  mode:c; tab-width:8; c-basic-offset:8; indent-tabs-mode:nil;  -*- */
/***************************************************************************/
/*
 * BGZIP FUSE Module
 * by Ronnie Sahlberg <ronniesahlberg@gmail.com>
 *
 * This is an overlay filesystem to transparently uncompress GZIP files
 * created by BGZIP and which have a matching .gz.gzi index file.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

#define _GNU_SOURCE
#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64

#include <dirent.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <htslib/bgzf.h>
#include <htslib/hts.h>
#include <tdb.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

#define LOG(...) {                          \
        if (logfile) { \
            FILE *fh = fopen(logfile, "a+"); \
            fprintf(fh, "[BGZIP] "); \
            fprintf(fh, __VA_ARGS__); \
            fclose(fh); \
        } \
}

struct file {
        BGZF *fh;
        int fd;
};

static char *logfile;

static struct tdb_context *tdb;

/* descriptor for the underlying directory */
static int dir_fd;

/* Absolute path for ~/.fuse-bgzip directory */
static char *cache_path;

/* This function takes a path to a file and returns true if this needs
 * bgzip unpacking.
 * For a file <file> we need to unpack the file if
 *   <file>        does not exist
 *   <file>.gz     exists
 *   <file>.gz.gzi exists
 * In that situation READDIR will just turn a single instance for the name
 * <file> and hide the entries for <file>.gz and <file>.gz.gzi
 *
 * IF all three files exist, we do not mutate any of the READDIR data
 * and return all three names. Then we just redirect all I/O to the unpacked
 * file. The reason we do not hide the <file>.gz and <file>.gz.gzi is to make
 * it possible for the user to see that something is odd/wrong and possibly
 * take action. (action == delete the unpacked file, it is redundant.)
 */
static int need_bgzip_uncompress(const char *file) {
        char stripped[PATH_MAX];
        char tmp[PATH_MAX];
        struct stat st;
        TDB_DATA key, data;
        uint8_t ret = 1;

        LOG("NEED_BGZIP_UNCOMPRESS [%s]\n", file);
        key.dptr = discard_const(file);
        key.dsize = strlen(file);
        data = tdb_fetch(tdb, key);
        if (data.dptr) {
                uint8_t val = data.dptr[0];
                free(data.dptr);
                return val;
        }

        snprintf(stripped, PATH_MAX,"%s", file);
        if (strlen(stripped) > 4 &&
            !strcmp(stripped + strlen(stripped) - 4, ".gzi")) {
                stripped[strlen(stripped) - 4] = 0;
        }
        if (strlen(stripped) > 3 &&
            !strcmp(stripped + strlen(stripped) - 3, ".gz")) {
                stripped[strlen(stripped) - 3] = 0;
        }

        if (fstatat(dir_fd, stripped, &st, AT_NO_AUTOMOUNT) == 0) {
                ret = 0;
                goto finished;
        }
        snprintf(tmp, PATH_MAX, "%s.gz", stripped);
        if (fstatat(dir_fd, tmp, &st, AT_NO_AUTOMOUNT) != 0) {
                ret = 0;
                goto finished;
        }
        snprintf(tmp, PATH_MAX, "%s.gz.gzi", stripped);
        if (fstatat(dir_fd, tmp, &st, AT_NO_AUTOMOUNT) != 0) {
                ret = 0;
                goto finished;
        }

finished:
        data.dptr = &ret;
        data.dsize = 1;
        tdb_store(tdb, key, data, TDB_REPLACE);
        return ret;
}

static void copy_index_file(const char *path)
{
        int in, out, count;
        char tmp[PATH_MAX];
        char buf[1024];
        char *ptr;

        in = openat(dir_fd, path, O_RDONLY);
        if (in == -1) {
                return;
        }

        /* strip any leading directories before we generate the cache file */
        ptr = strrchr(path, '/');
        if (ptr) {
                path = ptr + 1;
        }

        snprintf(tmp, PATH_MAX, "%s/%s", cache_path, path);
        out = open(tmp, O_WRONLY|O_CREAT, 0600);
        if (out == -1) {
                close(in);
                return;
        }

        while (1) {
                count = read(in, buf, sizeof(buf));
                if (count < 0) {
                        close(in);
                        close(out);
                        unlink(tmp);
                        return;
                }
                if (count == 0) {
                        break;
                }
                write(out, buf, count);
        }
        close(in);
        close(out);
}

static void load_index_file(BGZF *fh, const char *path)
{
        char tmp[PATH_MAX];
        const char *ptr;
        int ret;
        struct stat st;

        /* Create the full path to the index file. */
        ptr = strrchr(path, '/');
        if (ptr) {
                ptr++;
        } else {
                ptr = path;
        }
        snprintf(tmp, PATH_MAX, "%s/%s", cache_path, ptr);

        if (stat(tmp, &st) == -1 && errno == ENOENT) {
                copy_index_file(path);
        }

        ret = bgzf_index_load(fh, tmp, NULL);
        if (ret == -1) {
                LOG("LOAD_INDEX_FILE [%s] %s\n", tmp, strerror(errno));
        }
}

/* returns the size of the uncompressed file, or 0 if it could not be
 * determined.
 */
static off_t get_unzipped_size(const char *path)
{
        BGZF *fh;
        char cache[PATH_MAX];
        char tmp[PATH_MAX];
        char index_file[PATH_MAX];
        int fd;
        off_t pos;
        uint64_t start_pos;
        ssize_t count;
        char buf[4096];
        const char *ptr;

        /* Create the full path to the cache file. */
        ptr = strrchr(path, '/');
        if (ptr) {
                ptr++;
        } else {
                ptr = path;
        }
        snprintf(cache, PATH_MAX, "%s/%s.size", cache_path, ptr);

        /* Try reading size from cache */
        fd = open(cache, O_RDONLY);
        if (fd >= 0) {
                if (read(fd, &pos, sizeof(pos)) == sizeof(pos)) {
                        close(fd);
                        return pos;
                }
                close(fd);
        }

        snprintf(tmp, PATH_MAX, "%s.gz", path);
        fd = openat(dir_fd, tmp, O_RDONLY);
        if (fd == -1) {
                return 0;
        } 

        fh = bgzf_dopen(fd, "ru");
        if (fh == NULL) {
                close(fd);
                return 0;
        }
        snprintf(index_file, PATH_MAX, "%s.gz.gzi", path);
        load_index_file(fh, index_file);


        /* Now we need to peek into the index file to find the uncompressed
         * offset for the final gzip block of the index.
         *
         * The index file consists of
         * +------------------------------+
         * |            count             | 8 bytes
         * +------------------------------+
         * followed by count + 1 blocks of
         * +------------------------------+
         * |      compressed offset       | 8 bytes
         * +------------------------------+
         * |      uncompressed offset     | 8 bytes
         * +------------------------------+
         * So just reading the last 8 bytes id the index file will
         * give us a good (and valid) starting offset for finding the EOF and
         * uncompressed file size.
         */
        start_pos = 0;
        snprintf(index_file, PATH_MAX, "%s/%s.gz.gzi", cache_path, ptr);

        fd = open(index_file, O_RDONLY);
        if (fd == -1) {
                return 0;
        }
        read(fd, &start_pos, sizeof(start_pos));
        start_pos = le64toh(start_pos);
        pread(fd, &start_pos, 8, start_pos * 16);
        start_pos = le64toh(start_pos);
        close(fd);

        /* Seek to the start of the final gzip block */
        bgzf_useek(fh, start_pos, SEEK_SET);
        pos = start_pos;

        /* And start scanning for EOF */
        while (1) {
                count = bgzf_read(fh, buf, sizeof(buf));
                if (count < 0) {
                        bgzf_close(fh);
                        return 0;
                }
                if (count == 0) {
                        break;
                }
                pos += count;
        }
        bgzf_close(fh);

        /* Write the size to cache */
        fd = open(cache, O_CREAT|O_WRONLY, 0600);
        if (fd == -1) {
                return pos;
        }
        if (write(fd, &pos, sizeof(pos)) != sizeof(pos)) {
                close(fd);
                return pos;
        }
        close(fd);
        
        return pos;
}

static int fuse_bgzip_getattr(const char *path, struct stat *stbuf)
{
        int ret;

        if (path[0] == '/') {
                path++;
        }

        ret = fstatat(dir_fd, path, stbuf, AT_NO_AUTOMOUNT|AT_EMPTY_PATH);
        if (ret && errno == ENOENT) {
                if (need_bgzip_uncompress(path)) {
                        char tmp[PATH_MAX];
                        int fd;

                        snprintf(tmp, PATH_MAX, "%s.gz", path);
                        ret = fstatat(dir_fd, tmp, stbuf, AT_NO_AUTOMOUNT);
                        if (ret) {
                                LOG("GETATTR [%s] %s\n", path, strerror(errno));
                                return -errno;
                        }

                        stbuf->st_size = get_unzipped_size(path);
                        LOG("GETATTR [%s] SUCCESS\n", path);
                        return 0;
                }
        }
        if (ret) {
                LOG("GETATTR [%s] SUCCESS\n", path);
                return -errno;
        }
        LOG("GETATTR [%s] SUCCESS\n", path);
        return 0;
}

static int fuse_bgzip_read(const char *path, char *buf, size_t size,
                           off_t offset, struct fuse_file_info *ffi)
{
        struct file *file;
        int fd;
        int ret;
        
        if (path[0] == '/') {
                path++;
        }

        file = (void *)ffi->fh;
        if (file->fh) {                
                ret = bgzf_useek(file->fh, offset, SEEK_SET);
                if (ret == -1) {
                        LOG("READ useek:%p [%s] %jd:%zu %s\n", file->fh, path, offset, size, strerror(errno));
                        return -errno;
                }
                
                ret = bgzf_read(file->fh, buf, size);
                if (ret == -1) {
                        LOG("READ read [%s] %jd:%zu %s\n", path, offset, size, strerror(errno));
                        return -errno;
                }
                LOG("READ [%s] %jd:%zu %d\n", path, offset, size, ret);
                return ret;
        }
        
        /* Passthrough to underlying filesystem */
        ret = pread(file->fd, buf, size, offset);
        return (ret == -1) ? -errno : ret;
}

static int fuse_bgzip_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi)
{
        DIR *dir;
        struct dirent *ent;
        int fd;

        if (path[0] == '/') {
                path++;
        }
        if (path[0] == '\0') {
                path = ".";
        }

        LOG("READDIR [%s]\n", path);

        fd = openat(dir_fd, path, O_DIRECTORY);
        dir = fdopendir(fd);
        if (dir == NULL) {
                return -errno;
        }
        while ((ent = readdir(dir)) != NULL) {
                struct ecm *e = NULL;
                char full_path[PATH_MAX];

                if (strcmp(path, ".")) {
                        snprintf(full_path, PATH_MAX, "%s/%s",
                                 path, ent->d_name);
                } else {
                        strcpy(full_path, ent->d_name);
                }

                if (need_bgzip_uncompress(full_path)) {
                        char tmp[PATH_MAX];

                        snprintf(tmp, PATH_MAX, "%s", ent->d_name);
                        if (strlen(tmp) > 7 &&
                            !strcmp(tmp + strlen(tmp) - 7, ".gz.gzi")) {
                                tmp[strlen(tmp) - 7] = 0;
                                filler(buf, tmp, NULL, 0);
                        }
                        continue;
                }

                filler(buf, ent->d_name, NULL, 0);
        }
        closedir(dir);
        return 0;
}

static int fuse_bgzip_open(const char *path, struct fuse_file_info *ffi)
{
        struct stat st;
        int ret;
        struct file *file = malloc(sizeof(struct file));

        file->fh = NULL;
        file->fd = -1;

        if (path[0] == '/') {
                path++;
        }

        ret = fstatat(dir_fd, path, &st, AT_NO_AUTOMOUNT);
        if (ret && errno == ENOENT) {
                if (need_bgzip_uncompress(path)) {
                        char tmp[PATH_MAX];
                        int fd;

                        snprintf(tmp, PATH_MAX, "%s.gz", path);
                        fd = openat(dir_fd, tmp, O_RDONLY);
                        if (fd == -1) {
                                free(file);
                                LOG("OPEN BGZF openat [%s] ENOENT\n", path);
                                return -ENOENT;
                        }

                        file->fh = bgzf_dopen(fd, "ru");
                        if (file->fh == NULL) {
                                close(fd);
                                free(file);
                                LOG("OPEN BGZF bgzf_open [%s] ENOENT\n", path);
                                return -ENOENT;
                        }

                        snprintf(tmp, PATH_MAX, "%s.gz.gzi", path);
                        load_index_file(file->fh, tmp);

                        ffi->fh = (uint64_t)file;
                        return 0;
                }
        }

        file->fd = openat(dir_fd, path, O_RDONLY);
        if (file->fd == -1) {
                free(file);
                LOG("OPEN FD [%s] %s\n", path, strerror(errno));
                return -errno;
        }
        ffi->fh = (uint64_t)file;
        LOG("OPEN FD [%s] SUCCESS\n", path);
        return 0;
}

static int fuse_bgzip_release(const char *path, struct fuse_file_info *ffi)
{
        struct file *file = (struct file *)ffi->fh;
        
        if (path[0] == '/') {
                path++;
        }

        LOG("RELEASE [%s]\n", path);

        if (file == NULL) {
                return 0;
        }
        if (file->fh) {
                bgzf_close(file->fh);
        }
        if (file->fd != -1) {
                close(file->fd);
        }
        free(file);

        return 0;
}

static int fuse_bgzip_statfs(const char *path, struct statvfs* stbuf)
{
        return fstatvfs(dir_fd, stbuf);
}


static struct fuse_operations bgzip_oper = {
        .getattr        = fuse_bgzip_getattr,
        .open           = fuse_bgzip_open,
        .release        = fuse_bgzip_release,
        .read           = fuse_bgzip_read,
        .readdir        = fuse_bgzip_readdir,
        .statfs         = fuse_bgzip_statfs,
};

static void print_usage(char *name)
{
        printf("Usage: %s [-?|--help] [-a|--allow-other] "
               "[-m|--mountpoint=mountpoint] "
               "[-l|--logfile=logfile] [-f|--foreground]", name);
        exit(0);
}

int main(int argc, char *argv[])
{
        int c, ret = 0, opt_idx = 0;
        char *mnt = NULL;
        static struct option long_opts[] = {
                { "help", no_argument, 0, '?' },
                { "allow-other", no_argument, 0, 'a' },
                { "logfile", required_argument, 0, 'l' },
                { "mountpoint", required_argument, 0, 'm' },
                { "foreground", no_argument, 0, 'f' },
                { NULL, 0, 0, 0 }
        };
        int fuse_bgzip_argc = 6;
        char *fuse_bgzip_argv[16] = {
                "fuse-bgzip",
                "<export>",
                "-omax_write=32768",
                "-ononempty",
                "-s",
                "-odefault_permissions",
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
        };
        struct passwd *pw = getpwuid(getuid());
        const char *homedir = pw->pw_dir;
        struct stat st;
        char fs_name[1024], fs_type[1024];

        while ((c = getopt_long(argc, argv, "?hafl:m:", long_opts,
                    &opt_idx)) > 0) {
                switch (c) {
                case 'h':
                case '?':
                        print_usage(argv[0]);
                        return 0;
                case 'a':
                        fuse_bgzip_argv[fuse_bgzip_argc++] = "-oallow_other";
                        break;
                case 'f':
                        fuse_bgzip_argv[fuse_bgzip_argc++] = "-f";
                        break;
                case 'l':
                        logfile = strdup(optarg);
                        break;
                case 'm':
                        mnt = strdup(optarg);
                        break;
                }
        }

        snprintf(fs_name, sizeof(fs_name), "-ofsname=%s", mnt);
        fuse_bgzip_argv[fuse_bgzip_argc++] = fs_name;

        snprintf(fs_type, sizeof(fs_type), "-osubtype=BGUNZIP");
        fuse_bgzip_argv[fuse_bgzip_argc++] = fs_type;

        if (mnt == NULL) {
                fprintf(stderr, "-m was not specified.\n");
                print_usage(argv[0]);
                ret = 10;
                exit(1);
        }

        asprintf(&cache_path, "%s/.fuse-bgzip", homedir);
        if (stat(cache_path, &st) == -1 && errno == ENOENT) {
                if (mkdir(cache_path, 0700) == -1) {
                        exit(1);
                }
        }
        
        dir_fd = open(mnt, O_DIRECTORY);
        fuse_bgzip_argv[1] = mnt;

        tdb = tdb_open(NULL, 10000001, TDB_INTERNAL, O_RDWR, 0);
        if (tdb == NULL) {
                printf("Failed to open TDB\n");
                exit(1);
        }

        return fuse_main(fuse_bgzip_argc, fuse_bgzip_argv, &bgzip_oper, NULL);
}
