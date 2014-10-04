// Helper for streaming a HTTP[S] response using synchronous recv() style calls
// Under the hood, use libcurl multi interface with one curl handle. When
// recv() is called without sufficient data buffered, does curl_multi_perform
// to receive a chunk of of data (via CURLOPT_WRITEFUNCTION).
// Sort of like: http://curl.haxx.se/libcurl/c/fopen.html

// Note, libcurl is responsible for checking the size of the response body against
// the content-length header, if any. cf. http://curl.haxx.se/libcurl/c/libcurl-errors.html
//
//   CURLE_PARTIAL_FILE (18)
//
//     A file transfer was shorter or larger than expected. This happens when the
//     server first reports an expected transfer size, and then delivers data that
//     doesn't match the previously given size.

#include <curl/curl.h>
#include <string>
#include <map>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <unistd.h>
#include <deque>
#include <list>
#include <assert.h>
#include <algorithm>
#include <memory.h>
extern "C" {
#include "htslib/curlstream.h"
};

// trim from start
static inline std::string &ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
}
// trim from end
static inline std::string &rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    return s;
}
// trim from both ends
static inline std::string &trim(std::string &s) {
    return ltrim(rtrim(s));
}

static CURLcode ensure_curl_init() {
    static bool need_init = true;
    if (need_init) {
        CURLcode ans = curl_global_init(CURL_GLOBAL_ALL);
        need_init = false;
        return ans;
    }
    return CURLE_OK;
}

using headers = std::map<std::string,std::string>;

// Helper class for providing HTTP request headers to libcurl
class RequestHeadersHelper {
    std::list<std::string> bufs;
    curl_slist *slist_;
public:
    RequestHeadersHelper() : slist_(nullptr) {}

    void Init(const headers& headers) {
        assert(slist_ == nullptr);
        for(auto it = headers.cbegin(); it != headers.cend(); it++) {
            std::stringstream stm;
            stm << it->first << ": " << it->second;
            bufs.push_back(stm.str());
            slist_ = curl_slist_append(slist_, bufs.back().c_str());
        }
    }
    virtual ~RequestHeadersHelper() {
        if (slist_ != nullptr) curl_slist_free_all(slist_);
    }
    operator curl_slist*() const { return slist_; }
};

// for CURLOPT_HEADERFUNCTION: parse the headers
static size_t headerfunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    headers& h = *reinterpret_cast<headers*>(userdata);
    
    size_t sep;
    for (sep=0; sep<size; sep++) {
        if (ptr[sep] == ':') break;
    }

    std::string k, v;
    k.assign(ptr, sep);
    k = trim(k);

    if (k.size()) {
        if (sep < size-1) {
            v.assign(ptr+sep+1, size-sep-1);
            v = trim(v);
            if (v.size()) {
                // lowercase key
                std::transform(k.begin(), k.end(), k.begin(), ::tolower);
                h[k] = v;
            }
        }
    }

    return size;
}

// FIFO buffer for streaming the response body in chunks of user-defined size
class FifoBuffer {
    std::deque<std::string> q_;

public:
    bool empty() const {
        return q_.empty();
    }

    size_t size() const {
        size_t ans = 0;
        for (auto it : q_) {
            ans += it.size();
        }
        return ans;
    }

    // add to end of buffer
    void write(const char *s, size_t n) {
        if (n > 0) {
            q_.push_back(std::string(s,n));
        }
    }

    // consume some from beginning of buffer
    size_t readsome(char *s, size_t n) {
        if (empty() || n == 0) return 0;

        size_t fs = q_.front().size();
        size_t neff = std::min(fs,n);
        std::string remainder;

        memcpy(s, q_.front().c_str(), neff);
        if (neff < fs) {
            remainder = q_.front().substr(neff);
        }

        q_.pop_front();
        if (remainder.size() > 0) {
            q_.push_front(remainder);
        }

        return neff;
    }
};

// for CURLOPT_WRITEFUNCTION
static size_t writefunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    FifoBuffer *response_body = reinterpret_cast<FifoBuffer*>(userdata);
    response_body->write(ptr, size);
    return size;
}

// helper class keeping track of error codes & displaying error messages
class Status {
    int code_;

public:
    Status(int code, const char* msg = nullptr) : code_(code) {
        if (code_ < 0) {
            std::cerr << "HTTP stream: "
                      << (msg != nullptr ? msg : "Error")
                      << " (" << code_ << ")" << std::endl;
        }
    }

    Status(const Status& o) : code_(o.code_) {}

    bool ok() const { return code_ == 0; }
    bool bad() const { return !ok(); }
    operator int() const { return code_; }
};

class CurlStream {
    std::string url_;
    off_t ofs_;

    CURL *h_;
    CURLM *m_;

    long response_code_;
    
    RequestHeadersHelper request_headers_helper_;
    headers response_headers_;

    FifoBuffer buf_; // response body fifo

    CURLMsg *final_msg_;
    int final_read_rc_;

public:
    CurlStream()
        : ofs_(0), h_(nullptr), m_(nullptr)
        , response_code_(0)
        , final_msg_(nullptr), final_read_rc_(1)
        {}

    ~CurlStream() {
        if (h_) { 
            assert(m_ != nullptr);
            curl_multi_remove_handle(m_, h_);
            curl_easy_cleanup(h_);
        }

        curl_multi_cleanup(m_);
    }

    int open(std::string url, off_t ofs = 0) {
        if (m_ || h_) return Status(-2,"multiple open() calls");

        // prepare request
        headers request_headers;
        if (ofs) {
            std::stringstream stm;
            stm << "bytes=" << ofs << "-";
            request_headers["range"] = stm.str();
        }

        Status s = prepare(url, request_headers);
        if (s.bad()) return s;

        // initialize curl
        if (ensure_curl_init() != CURLE_OK)
            return Status(-1,"curl_global_init failed");
        m_ = curl_multi_init();
        if (!m_) return Status(-1,"curl_multi_init failed");
        h_ = curl_easy_init();
        if (!h_) return Status(-1,"curl_easy_init failed");

        if (curl_multi_add_handle(m_, h_) != CURLM_OK) {
            curl_easy_cleanup(h_);
            h_ = nullptr;
            // m_ cleaned up in destructor
            return Status(-1,"curl_multi_add_handle failed");
        }

        // configure request
        #define CURLSETOPT(x,y) if (curl_easy_setopt(h_, x, y) != CURLE_OK) return -1

        CURLSETOPT(CURLOPT_URL, url.c_str());
        request_headers_helper_.Init(request_headers);
        CURLSETOPT(CURLOPT_HTTPHEADER, (curl_slist*)request_headers_helper_);

        CURLSETOPT(CURLOPT_WRITEHEADER, &response_headers_);
        CURLSETOPT(CURLOPT_HEADERFUNCTION, headerfunction);

        CURLSETOPT(CURLOPT_WRITEDATA, &buf_);
        CURLSETOPT(CURLOPT_WRITEFUNCTION, writefunction);

        CURLSETOPT(CURLOPT_FOLLOWLOCATION, 1);
        CURLSETOPT(CURLOPT_MAXREDIRS, 16);

        // run the request at least until we get the HTTP response code
        s = perform();
        if (s.bad()) return s;

        while (final_msg_ == nullptr && response_code_ == 0) {
            s = block();
            if (s.bad()) return s;
            s = perform();
            if (s.bad()) return s;
        }

        if (final_msg_ != nullptr) {
            s = final_status();
            if (s.bad()) {
                final_read_rc_ = -1;
                return s;
            }
        }

        assert(response_code_ != 0);
        if (response_code_ < 200 || response_code_ > 299) {
            std::stringstream stm;
            stm << "HTTP response code " << response_code_;
            return Status(-4,stm.str().c_str());
        }
        if (ofs > 0 && response_code_ != 206) {
            std::stringstream stm;
            stm << "HTTP response code " << response_code_
                << " instead of 206 to range request";
            return Status(-8,stm.str().c_str());
        }

        return 0;
    }

    long response_code() const {
        return response_code_;
    }

    ssize_t read(void *buf, size_t n) {
        if (final_read_rc_ <= 0) return final_read_rc_;
        if (!h_) return Status(-2,"premature read()");
        if (response_code_ < 200 || response_code_ > 299)
            return Status(-2,"read() on failed stream");

        if (buf_.empty()) {
            // perform until we've either buffered some data or finished
            // receiving the response
            Status s = perform();
            if (s.bad()) return s;
            while (final_msg_ == nullptr && buf_.empty()) {
                s = block();
                if (s.bad()) return s;
                s = perform();
                if (s.bad()) return s;
            }

            if (buf_.empty()) {
                assert(final_msg_ != nullptr);
                // all done
                final_read_rc_ = final_status();
                return final_read_rc_;
            }
        }

        // read some
        return (ssize_t) buf_.readsome((char*)buf, n);
    }

protected:
    Status prepare(std::string& url, headers& request_headers) {
        return Status(0);
    }

private:
    // curl_multi_perform with side effects:
    // - set response_code_ asap
    // - set final_msg_ when request/response is finished (success or error)
    // An error might be indicated either by bad return status, or by
    // erroneous CURLcode final_msg_->data.result
    Status perform() {
        if (final_msg_ != nullptr) {
            return Status(0);
        }

        int still_running = 0;
        CURLMcode mc = curl_multi_perform(m_, &still_running);
        if (mc != CURLM_OK) return Status(-1,curl_multi_strerror(mc));

        if (response_code_ == 0) {
            if (curl_easy_getinfo(h_, CURLINFO_RESPONSE_CODE, &response_code_) != CURLE_OK)
                return Status(-1,"couldn't get CURLINFO_RESPONSE_CODE");
            // "The value will be zero if no server response code has been received."
        }

        if (still_running == 0) {
            CURLMsg *msg;
            int msgs=0;
            while ((msg = curl_multi_info_read(m_, &msgs))) {
                if (msg->msg == CURLMSG_DONE) {
                    final_msg_ = msg;
                }
            }
            assert(final_msg_ != nullptr);
        }

        return Status(0);
    }

    // once final_msg_ is known, retrieve final_msg_->data.result as Status
    Status final_status() {
        assert(final_msg_ != nullptr);
        if (final_msg_->data.result == CURLE_OK) return Status(0);
        return Status(-1,curl_easy_strerror(final_msg_->data.result));
    }

    // blocking select() based on http://curl.haxx.se/libcurl/c/multi-app.html
    Status block() {
        timeval timeout;
     
        fd_set fdread;
        fd_set fdwrite;
        fd_set fdexcep;
        int maxfd = -1;
     
        long curl_timeo = -1;
     
        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);
     
        /* set a suitable timeout to play around with */ 
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
     
        if (curl_multi_timeout(m_, &curl_timeo) != CURLM_OK)
            return Status(-1,"curl_multi_timeout failure");
        if (curl_timeo >= 0) {
          timeout.tv_sec = curl_timeo / 1000;
          if(timeout.tv_sec > 1)
            timeout.tv_sec = 1;
          else
            timeout.tv_usec = (curl_timeo % 1000) * 1000;
        }
     
        /* get file descriptors from the transfers */ 
        if (curl_multi_fdset(m_, &fdread, &fdwrite, &fdexcep, &maxfd) != CURLM_OK)
            return Status(-1,"curl_multi_fdset failure");
     
        if (select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout) < 0)
            return Status(-1,"select() failure");
 
        return Status(0);
    }
};

// C API

#include <errno.h>

struct CurlStreamBox {
    std::string url;
    off_t ofs;
    CurlStream *stm;

    CurlStreamBox() : ofs(0), stm(nullptr) {}
};

int open_helper(CURLSTREAM s) {
    if (s->stm) {
        delete s->stm;
    }

    s->stm = new CurlStream;

    int c = s->stm->open(s->url, s->ofs);
    if (c == 0)
        return 0;

    long ret = s->stm->response_code();
    switch (ret) {
    case 401: errno = EPERM; break;
    case 403: errno = EACCES; break;
    case 404: errno = ENOENT; break;
    case 407: errno = EPERM; break;
    case 408: errno = ETIMEDOUT; break;
    case 410: errno = ENOENT; break;
    case 503: errno = EAGAIN; break;
    case 504: errno = ETIMEDOUT; break;
    default:  errno = (ret >= 400 && ret < 500)? EINVAL : EIO; break;
    }

    delete s->stm;
    s->stm = nullptr;
    return c;
}

extern "C" int curlstream_open(const char *url, off_t ofs, CURLSTREAM *s) {
    CurlStreamBox *box = new CurlStreamBox;
    box->url = url;
    box->ofs = ofs;

    int c = open_helper(box);

    if (c != 0) {
        delete box;
        return c;
    }

    *s = box;
    return 0;
}

extern "C" ssize_t curlstream_read(CURLSTREAM s, void *buf, size_t n) {
    int c;
    if (s->stm == nullptr) {
        c = open_helper(s);
        if (c != 0) return c;
    }
    c = s->stm->read(buf,n);
    if (c != 0) {
        errno = EIO; // TODO perhaps EPIPE on CURLE_PARTIAL_FILE
    }
    return c;
}

extern "C" off_t curlstream_seek(CURLSTREAM s, off_t ofs, int whence) {
    if (whence == SEEK_SET) {
        s->ofs = ofs;
    } else if (whence == SEEK_CUR) {
        s->ofs += ofs;
    } else if (whence == SEEK_END) {
        errno = ESPIPE;
        return -1;
    } else {
        return -1;
    }

    if (s->stm != nullptr) {
        delete s->stm;
        s->stm = nullptr;
    }

    return s->ofs;
}

extern "C" void curlstream_close(CURLSTREAM s) {
    if (s->stm != nullptr) {
        delete s->stm;
    }
    delete s;
}
