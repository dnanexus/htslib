/* Initiate libcurl HTTP[S] requests and stream the response body via blocking
   read calls. */

#ifndef CURLSTREAM_H
#define CURLSTREAM_H

#include <sys/types.h>

typedef struct CurlStreamBox *CURLSTREAM;

/* Initiate a GET request to the url.
   If ofs>0, supplies a 'range: (ofs)-' header.
   On successful return (0), a 2xx response code has been received, and you
   can begin reading the body. */
int curlstream_open(const char* url, off_t ofs, CURLSTREAM *s);

/* Block until some data is available, if necessary, and read up to n bytes
   of the response body into buf. Returns the number of bytes actually read,
   or zero to indicate the response body has been completed successfully.
   Negative return value indicates error. Note, libcurl does check the
   received response body size against the content-length header, if
   present, and will error out if they do not match. */
ssize_t curlstream_read(CURLSTREAM s, void *buf, size_t n);

/* Seek to a different offset in the remote file. */
off_t curlstream_seek(CURLSTREAM s, off_t ofs, int whence);

void curlstream_close(CURLSTREAM s);

#endif
