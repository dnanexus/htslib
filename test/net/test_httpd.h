/* test_httpd.c: a mock http server (using libmicrohttpd) against which the htslib
 * http client can be tested
 */

#ifndef TEST_HTTPD_H
#define TEST_HTTPD_H

#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdarg.h>
#include <microhttpd.h>

#include <string>
#include <map>
#include <memory>

class TestHTTPd {
	unsigned short port_;
	std::map<std::string,std::string> files_;
	MHD_Daemon *d_;
	unsigned int requests_to_fail_;
	ssize_t truncate_response_;

	friend int on_request(void *cls, struct MHD_Connection *connection,
                     const char *url, const char *method,
                     const char *version, const char *upload_data,
                     size_t *upload_data_size, void **con_cls);

	int OnRequest(MHD_Connection *connection,
                     const char *url, const char *method,
                     const char *version, const char *upload_data,
                     size_t *upload_data_size, void **con_cls);

public:
	TestHTTPd() : d_(nullptr), requests_to_fail_(0), truncate_response_(-1) {}
	virtual ~TestHTTPd();

	bool Start(unsigned short port, const std::map<std::string,std::string>& files);
	void FailNextRequests(unsigned int n) { requests_to_fail_ = n; }
	void TruncateNextResponse(size_t offset) { truncate_response_ = (ssize_t) offset; }
	void Stop();
};

#endif