/*  hfile_net.c -- network backend for low-level input/output streams.

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

#include <stdlib.h>
#include <errno.h>

#include "hfile_internal.h"

#include "htslib/curlstream.h"

typedef struct {
    hFILE base;
    CURLSTREAM netfp;
} hFILE_net;

static ssize_t net_read(hFILE *fpv, void *buffer, size_t nbytes)
{
    hFILE_net *fp = (hFILE_net *) fpv;
    return curlstream_read(fp->netfp, buffer, nbytes);
}

static off_t net_seek(hFILE *fpv, off_t offset, int whence)
{
    hFILE_net *fp = (hFILE_net *) fpv;
    return curlstream_seek(fp->netfp, offset, whence);
}

static int net_close(hFILE *fpv)
{
    hFILE_net *fp = (hFILE_net *) fpv;
    curlstream_close(fp->netfp);
    return 0;
}

static const struct hFILE_backend net_backend =
{
    net_read, NULL, net_seek, NULL, net_close
};

hFILE *hopen_net(const char *filename, const char *mode)
{
    hFILE_net *fp;

    fp = (hFILE_net *) hfile_init(sizeof (hFILE_net), mode, 0);
    if (fp == NULL) return NULL;

    if (curlstream_open(filename, 0, &(fp->netfp)) != 0) {
        hfile_destroy((hFILE *) fp);
        return NULL;
    }

    fp->base.backend = &net_backend;
    return &fp->base;
}
