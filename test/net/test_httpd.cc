#include "test_httpd.h"
#include <iostream>
#include <sstream>
#include <fstream>
using namespace std;

int on_request(void *cls, struct MHD_Connection *connection,
                 const char *url, const char *method,
                 const char *version, const char *upload_data,
                 size_t *upload_data_size, void **con_cls)
{
    TestHTTPd *d = reinterpret_cast<TestHTTPd*>(cls);
    return d->OnRequest(connection, url, method, version, upload_data, upload_data_size, con_cls);
}

TestHTTPd::~TestHTTPd() {
    Stop();
}

bool TestHTTPd::Start(unsigned short port, const map<string,string>& files) {
    if (d_) {
        cerr << "TestHTTPd::Start: daemon already running" << endl;
        return false;
    }

    port_ = port;
    files_ = files;
    d_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY | MHD_USE_THREAD_PER_CONNECTION,
                          port, nullptr, nullptr, &on_request, this, MHD_OPTION_END);

    if (!d_) {
        cerr << "TestHTTPd::Start: MHD_start_daemon failed" << endl;
        return false;
    }

    return true;
}

void TestHTTPd::Stop() {
    if (d_) {
        MHD_stop_daemon(d_);
        d_ = nullptr;
    }
}

class TestHTTPd_Response {
    int fd_;
    size_t ofs_, len_;

public:
    TestHTTPd_Response(int fd, size_t ofs, size_t len)
        : fd_(fd)
        , ofs_(ofs)
        , len_(len)
        {}

    ~TestHTTPd_Response() {
        close(fd_);
    }

    int ContentReaderCallback(uint64_t pos, char *buf, size_t max) {
        if (pos >= len_) {
            return MHD_CONTENT_READER_END_WITH_ERROR;
        }
        if (lseek(fd_, ofs_+pos, SEEK_SET) < 0) {
            return MHD_CONTENT_READER_END_WITH_ERROR;
        }
        return read(fd_, buf, min(uint64_t(len_)-pos,max));
    }
};
long TestHTTPd_Response_ContentReaderCallback(void *cls, uint64_t pos, char *buf, size_t max) {
    return reinterpret_cast<TestHTTPd_Response*>(cls)->ContentReaderCallback(pos, buf, max);
}
void TestHTTPd_Response_ContentReaderFreeCallback(void *cls) {
    auto p = reinterpret_cast<TestHTTPd_Response*>(cls);
    delete p;
}

bool get_range_header(MHD_Connection *connection, size_t& lo, size_t& hi, const size_t sz) {
    const char* crangehdr = MHD_lookup_connection_value(connection, MHD_HEADER_KIND, "range");
    if (!crangehdr) return false;
    string rangehdr(crangehdr);
    if (rangehdr.size() < 8 || rangehdr.substr(0,6) != "bytes=") return false;
    size_t dashpos = rangehdr.find('-');
    if (dashpos == string::npos || rangehdr.rfind('-') != dashpos || dashpos < 7) return false;
    string slo = rangehdr.substr(6,dashpos-6);
    lo = strtoull(slo.c_str(), nullptr, 10);
    if (dashpos < rangehdr.size()-1) {
        string shi = rangehdr.substr(dashpos+1);
        hi = strtoull(shi.c_str(), nullptr, 10);
        return hi >= lo;
    } else {
        // range header is of the form "range: bytes=123-"
        // set hi to the end of the file
        hi = sz-1;
        return true;
    }
}

int TestHTTPd::OnRequest(MHD_Connection *connection,
                         const char *url, const char *method,
                         const char *version, const char *upload_data,
                         size_t *upload_data_size, void **con_cls) {
    MHD_Response *response = nullptr;
    unsigned int response_code = 404;
    int ret;

    if (requests_to_fail_ == 0) {
        auto entry = files_.find(url);
        if (entry != files_.end()) {
            int fd = open(entry->second.c_str(), O_RDONLY);
            if (fd > 0) {
                struct stat st;
                if (fstat(fd,&st) == 0) {
                    size_t lo, hi;
                    if (get_range_header(connection,lo,hi,st.st_size)) {
                        if (lo < size_t(st.st_size)) {
                            response_code = 206;
                            // TODO set content-range response header, "lo-hi/st.st_size"
                            size_t actual_response_length = truncate_response_ >= 0 ? min(size_t(truncate_response_),hi-lo+1) : hi-lo+1;
                            truncate_response_ = -1;
                            //cout << "206 " << hi-lo+1 << " " << actual_response_length << endl;
                            if (!(response = MHD_create_response_from_callback(hi-lo+1, 1048576,
                                                                               &TestHTTPd_Response_ContentReaderCallback,
                                                                               new TestHTTPd_Response(fd, lo, actual_response_length),
                                                                               &TestHTTPd_Response_ContentReaderFreeCallback))) {
                                return MHD_NO;
                            }
                        } else response_code = 416;
                    } else {
                        response_code = 200;
                        size_t actual_response_length = truncate_response_ >= 0 ? min(truncate_response_,st.st_size) : st.st_size;
                        truncate_response_ = -1;
                        //cout << "200 " << st.st_size << " " << actual_response_length << endl;
                        if (!(response = MHD_create_response_from_callback(st.st_size, 1048576,
                                                                           &TestHTTPd_Response_ContentReaderCallback,
                                                                           new TestHTTPd_Response(fd, 0, actual_response_length),
                                                                           &TestHTTPd_Response_ContentReaderFreeCallback))) {
                            return MHD_NO;
                        }
                    }
                } else response_code = 500;
            }
        }
    } else {
        response_code = 500;
        --requests_to_fail_;
    }

    if (response == nullptr) {
        if (!(response = MHD_create_response_from_buffer(0, (void*) "", MHD_RESPMEM_PERSISTENT))) return MHD_NO;
    }
    ret = MHD_queue_response(connection, response_code, response);
    MHD_destroy_response(response);

    return ret;
}
