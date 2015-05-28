/*  test/hfile.c -- Test cases for low-level input/output streams.

    Copyright (C) 2013-2014 Genome Research Ltd.

    Author: John Marshall <jm18@sanger.ac.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.  */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/stat.h>

#include "htslib/hfile.h"
#include "htslib/hts_defs.h"

void HTS_NORETURN fail(const char *format, ...)
{
    int err = errno;
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    if (err != 0) fprintf(stderr, ": %s", strerror(err));
    fprintf(stderr, "\n");
    exit(EXIT_FAILURE);
}

void check_offset(hFILE *f, off_t off, const char *message)
{
    off_t ret = htell(f);
    if (ret < 0) fail("htell(%s)", message);
    if (ret == off) return;

    fprintf(stderr, "%s offset incorrect: expected %ld but got %ld\n",
            message, (long)off, (long)ret);
    exit(EXIT_FAILURE);
}

char *slurp(const char *filename)
{
    char *text;
    struct stat sbuf;
    size_t filesize;
    FILE *f = fopen(filename, "r");
    if (f == NULL) fail("fopen(\"%s\", \"r\")", filename);
    if (fstat(fileno(f), &sbuf) != 0) fail("fstat(\"%s\")", filename);
    filesize = sbuf.st_size;

    text = (char *) malloc(filesize + 1);
    if (text == NULL) fail("malloc(text)");

    if (fread(text, 1, filesize, f) != filesize) fail("fread");
    fclose(f);

    text[filesize] = '\0';
    return text;
}

hFILE *fin = NULL;
hFILE *fout = NULL;

void reopen(const char *infname, const char *outfname)
{
    if (fin) { if (hclose(fin) != 0) fail("hclose(input)"); }
    if (fout) { if (hclose(fout) != 0) fail("hclose(output)"); }

    fin = hopen(infname, "r");
    if (fin == NULL) fail("hopen(\"%s\")", infname);

    fout = hopen(outfname, "w");
    if (fout == NULL) fail("hopen(\"%s\")", outfname);
}

int main(void)
{
    static const int size[] = { 1, 13, 403, 999, 30000 };

    char buffer[40000];
    char *original;
    int c, i;
    ssize_t n;
    off_t off;
    char memfname[100];
    char* mbuffer;
    char** pmbuffer;
    size_t mlength;
    size_t* pmlength;

    reopen("vcf.c", "test/hfile1.tmp");
    while ((c = hgetc(fin)) != EOF) {
        if (hputc(c, fout) == EOF) fail("hputc");
    }
    if (herrno(fin)) { errno = herrno(fin); fail("hgetc"); }

    reopen("test/hfile1.tmp", "test/hfile2.tmp");
    if (hpeek(fin, buffer, 50) < 0) fail("hpeek");
    while ((n = hread(fin, buffer, 17)) > 0) {
        if (hwrite(fout, buffer, n) != n) fail("hwrite");
    }
    if (n < 0) fail("hread");

    reopen("test/hfile2.tmp", "test/hfile3.tmp");
    while ((n = hread(fin, buffer, sizeof buffer)) > 0) {
        if (hwrite(fout, buffer, n) != n) fail("hwrite");
        if (hpeek(fin, buffer, 700) < 0) fail("hpeek");
    }
    if (n < 0) fail("hread");

    reopen("test/hfile3.tmp", "test/hfile4.tmp");
    i = 0;
    off = 0;
    while ((n = hread(fin, buffer, size[i++ % 5])) > 0) {
        off += n;
        buffer[n] = '\0';
        check_offset(fin, off, "pre-peek");
        if (hputs(buffer, fout) == EOF) fail("hputs");
        if ((n = hpeek(fin, buffer, size[(i+3) % 5])) < 0) fail("hpeek");
        check_offset(fin, off, "post-peek");
    }
    if (n < 0) fail("hread");

    reopen("test/hfile4.tmp", "test/hfile5.tmp");
    n = hread(fin, buffer, 200);
    if (n < 0) fail("hread");
    else if (n != 200) fail("hread only got %d", (int)n);
    if (hwrite(fout, buffer, 1000) != 1000) fail("hwrite");
    check_offset(fin, 200, "input/first200");
    check_offset(fout, 1000, "output/first200");

    if (hseek(fin, 800, SEEK_CUR) < 0) fail("hseek/cur");
    check_offset(fin, 1000, "input/seek");
    for (off = 1000; (n = hread(fin, buffer, sizeof buffer)) > 0; off += n)
        if (hwrite(fout, buffer, n) != n) fail("hwrite");
    if (n < 0) fail("hread");
    check_offset(fin, off, "input/eof");
    check_offset(fout, off, "output/eof");

    if (hseek(fin, 200, SEEK_SET) < 0) fail("hseek/set");
    if (hseek(fout, 200, SEEK_SET) < 0) fail("hseek(output)");
    check_offset(fin, 200, "input/backto200");
    check_offset(fout, 200, "output/backto200");
    n = hread(fin, buffer, 800);
    if (n < 0) fail("hread");
    else if (n != 800) fail("hread only got %d", (int)n);
    if (hwrite(fout, buffer, 800) != 800) fail("hwrite");
    check_offset(fin, 1000, "input/wrote800");
    check_offset(fout, 1000, "output/wrote800");

    if (hflush(fout) == EOF) fail("hflush");

    original = slurp("vcf.c");
    for (i = 1; i <= 5; i++) {
        char *text;
        sprintf(buffer, "test/hfile%d.tmp", i);
        text = slurp(buffer);
        if (strcmp(original, text) != 0) {
            fprintf(stderr, "%s differs from vcf.c\n", buffer);
            return EXIT_FAILURE;
        }
        free(text);
    }
    free(original);

    if (hclose(fin) != 0) fail("hclose(input)");
    if (hclose(fout) != 0) fail("hclose(output)");

    fout = hopen("test/hfile_chars.tmp", "w");
    if (fout == NULL) fail("hopen(\"test/hfile_chars.tmp\")");
    for (i = 0; i < 256; i++)
        if (hputc(i, fout) != i) fail("chars: hputc (%d)", i);
    if (hclose(fout) != 0) fail("hclose(test/hfile_chars.tmp)");

    fin = hopen("test/hfile_chars.tmp", "r");
    if (fin == NULL) fail("hopen(\"test/hfile_chars.tmp\") for reading");
    for (i = 0; i < 256; i++)
        if ((c = hgetc(fin)) != i)
            fail("chars: hgetc (%d = 0x%x) returned %d = 0x%x", i, i, c, c);
    if ((c = hgetc(fin)) != EOF) fail("chars: hgetc (EOF) returned %d", c);
    if (hclose(fin) != 0) fail("hclose(test/hfile_chars.tmp) for reading");

    fin = hopen("data:hello, world!\n", "r");
    if (fin == NULL) fail("hopen(\"data:...\")");
    n = hread(fin, buffer, 300);
    if (n < 0) fail("hread");
    buffer[n] = '\0';
    if (strcmp(buffer, "hello, world!\n") != 0) fail("hread result");
    if (hclose(fin) != 0) fail("hclose(\"data:...\")");

    mbuffer = 0;
    pmbuffer = &mbuffer;
    mlength = 999999;
    pmlength = &mlength;

    strcpy(memfname, "mem:");
    memcpy((char*)&memfname+4, &pmbuffer, sizeof(void**));
    memcpy((char*)&memfname+4+sizeof(void**), &pmlength, sizeof(void**));

    fout = hopen(memfname, "w");
    if (fout == NULL) fail("hopen(\"mem:...\")");
    strcpy(buffer, "hello, world!\n");
    n = hwrite(fout, buffer, 14);
    if (n != 14) fail("mem hwrite");
    if (htell(fout) != 14) fail("htell");
    if (hclose(fout) != 0) fail("hclose(mem)");

    if (mbuffer == 0) fail("mem mbuffer");
    if (mlength < 14) fail("mem mlength");
    if (memcmp(mbuffer, "hello, world!\n", 14) != 0) fail("mem cmp");

    mlength = 14;
    fin = hopen(memfname, "r");
    if (fin == NULL) fail("hopen(\"mem:...\",\"r\")");
    n = hread(fin, buffer, 300);
    if (n < 0) fail("hread");
    buffer[n] = '\0';
    if (strcmp(buffer, "hello, world!\n") != 0) fail("hread result");
    if (hclose(fin) != 0) fail("hclose(\"data:...\")");

    fout = hopen(memfname, "w");
    if (fout == NULL) fail("hopen(\"mem:...\") MB");
    for (i = 0; i < (2<<20); i++) {
        buffer[0] = i % 128;
        if(hwrite(fout, buffer, 1) != 1) fail("mem hwrite MB");
    }
    if (htell(fout) != 2<<20) fail("mem htell MB");
    if (hclose(fout) != 0) fail("hclose mem MB");

    if (mlength < 2<<20) fail("mem mlength MB");
    fin = hopen(memfname, "r");
    if (fin == NULL) fail("hopen(\"mem:...\",\"r\") MB");
    for (i = 1048575; i >= 0; i--) {
        char c;
        if (hseek(fin, i, SEEK_SET) < 0) fail("mem hseek/set MB");
        if (hread(fin, &c, 1) != 1) fail("mem hread MB");
        if (c != i % 128) fail("mem result MB");
    }
    if (hclose(fin) != 0) fail("hclose(\"data:...\")");

    return EXIT_SUCCESS;
}
