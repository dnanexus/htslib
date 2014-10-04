#include <iostream>
#include <assert.h>
#include "htslib/hfile.h"
#include "htslib/bgzf.h"
#include "htslib/sam.h"
#include "htslib/hts.h"
#include "test_httpd.h"
#define CATCH_CONFIG_MAIN
#include "build/catch.hpp"
using namespace std;

const unsigned short TEST_HTTPD_PORT=8378;
const std::string TEST_BAM="/NA18508.chr20.test.bam";
const std::string TEST_BAM_URL="http://localhost:8378" + TEST_BAM;
const size_t TEST_BAM_SIZE = 2865846;
const size_t TEST_BAM_RECORDS = 42328;
// An offset into the test BAM coinciding with a BGZF block boundary
const size_t TEST_BAM_BGZF_BLOCK_BOUNDARY = 75776;

class MyTestHTTPd : public TestHTTPd {
    map<string,string> httpfiles_;
public:
    MyTestHTTPd() {
        httpfiles_[TEST_BAM] = "data" + TEST_BAM;
        httpfiles_[TEST_BAM + ".bai"] = "data" + TEST_BAM + ".bai";
        bool ok = Start(TEST_HTTPD_PORT,httpfiles_);
        assert(ok);
    }
};

// read to the end of the open hfile, ignore the contents, return # bytes
ssize_t hfile_dropall(hFILE *fp) {
    const size_t bufsz = 1048576;
    char buf[bufsz];
    ssize_t total = 0, readsz;

    while ((readsz = hread(fp, buf, bufsz))) {
        if (readsz < 0) {
            return -1;
        }
        total += readsz;
    }

    return total;
}

TEST_CASE("hfile full read") {
    MyTestHTTPd httpd;
    hFILE* fp = hopen(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);
    REQUIRE(TEST_BAM_SIZE == hfile_dropall(fp));
    hclose(fp);
}

TEST_CASE("hfile partial read") {
    MyTestHTTPd httpd;
    const size_t ofs = 1048576;
    hFILE* fp = hopen(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);

    REQUIRE(ofs == hseek(fp,ofs,SEEK_SET));
    REQUIRE((TEST_BAM_SIZE-ofs) == hfile_dropall(fp));  
    hclose(fp);
}

TEST_CASE("hfile truncated full read") {
    MyTestHTTPd httpd;
    httpd.TruncateNextResponse(1048576);
    hFILE* fp = hopen(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);
    // we should get errors here due to the truncated response
    REQUIRE(hfile_dropall(fp) < 0);
    REQUIRE(herrno(fp) != 0);
}

TEST_CASE("hfile truncated partial read") {
    MyTestHTTPd httpd;
    const size_t ofs = 1048576;
    hFILE* fp = hopen(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);

    httpd.TruncateNextResponse(ofs);
    REQUIRE(hseek(fp,ofs,SEEK_SET) == ofs);

    // we should get errors here due to the truncated response
    REQUIRE(hfile_dropall(fp) < 0);
    REQUIRE(herrno(fp) != 0);
}

TEST_CASE("bam full read") {
    MyTestHTTPd httpd;
    bam1_t *b = bam_init1();
    BGZF* fp = bgzf_open(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);
    unsigned int ct = 0;

    bam_hdr_t *hdr = bam_hdr_read(fp);;
    REQUIRE(hdr != nullptr);
    bam_hdr_destroy(hdr);

    int rc;
    while ((rc = bam_read1(fp,b)) >= 0) {
        ct++;
    }
    REQUIRE(rc == -1);

    bam_destroy1(b);
    bgzf_close(fp);

    REQUIRE(ct == TEST_BAM_RECORDS);
}

// Test a requested full read of the BAM with response truncated at several
// positions surrounding a BGZF block boundary
TEST_CASE("bam truncated full read") {
    MyTestHTTPd httpd;
    bam1_t *b = bam_init1();
    for (size_t trunc = TEST_BAM_BGZF_BLOCK_BOUNDARY-4; trunc <= TEST_BAM_BGZF_BLOCK_BOUNDARY+4; trunc++) {
        httpd.TruncateNextResponse(trunc);
        BGZF* fp = bgzf_open(TEST_BAM_URL.c_str(),"r");
        REQUIRE(fp != nullptr);
        unsigned int ct = 0;

        bam_hdr_t *hdr = bam_hdr_read(fp);;
        REQUIRE(hdr != nullptr);
        bam_hdr_destroy(hdr);

        int rc;
        while ((rc = bam_read1(fp,b)) >= 0) {
            ct++;
        }
        if (rc >= -1) {
            cout << trunc << " " << rc << " " << endl;
        }
        REQUIRE(rc < -1);
        bgzf_close(fp);
    }

    bam_destroy1(b);
}

// retrieval from indexed BAM
TEST_CASE("bam partial read") {
    MyTestHTTPd httpd;
    samFile* fp = sam_open(TEST_BAM_URL.c_str(),"r");
    REQUIRE(fp != nullptr);

    bam_hdr_t *header = sam_hdr_read(fp);
    REQUIRE(header != nullptr);

    hts_idx_t *idx = sam_index_load(fp, TEST_BAM_URL.c_str()); // load index
    REQUIRE(idx != 0);

    hts_itr_t *iter = sam_itr_querys(idx, header, "20:100000-110000"); // parse a region in the format like `chr2:100-200'
    REQUIRE(iter != 0);

    int rc;
    unsigned int ct = 0;
    bool any_outside = false;
    bam1_t *b = bam_init1();
    while ((rc = sam_itr_next(fp, iter, b)) >= 0) {
        ct++;
        if (b->core.pos < 99000 || b->core.pos > 110000) {
            any_outside = true;
        }
    }
    REQUIRE(rc == -1);
    REQUIRE(ct == 4312);
    REQUIRE(!any_outside);

    sam_itr_destroy(iter);
    bam_hdr_destroy(header);
    bam_destroy1(b);
    hts_idx_destroy(idx);
    sam_close(fp);
}

TEST_CASE("bam truncated partial read") {
    MyTestHTTPd httpd;
    for (size_t trunc = TEST_BAM_BGZF_BLOCK_BOUNDARY-1630; trunc <= TEST_BAM_BGZF_BLOCK_BOUNDARY-1620; trunc++) {
        samFile* fp = sam_open(TEST_BAM_URL.c_str(),"r");
        REQUIRE(fp != nullptr);

        bam_hdr_t *header = sam_hdr_read(fp);
        REQUIRE(header != nullptr);

        hts_idx_t *idx = sam_index_load(fp, TEST_BAM_URL.c_str()); // load index
        REQUIRE(idx != 0);

        hts_itr_t *iter = sam_itr_querys(idx, header, "20:100000-110000"); // parse a region in the format like `chr2:100-200'
        REQUIRE(iter != 0);

        int rc;
        unsigned int ct = 0;
        bool any_outside = false;
        bam1_t *b = bam_init1();
        httpd.TruncateNextResponse(trunc);
        while ((rc = sam_itr_next(fp, iter, b)) >= 0) {
            ct++;
            if (b->core.pos < 99000 || b->core.pos > 110000) {
                any_outside = true;
            }
        }
        REQUIRE(rc < -1);
        REQUIRE(!any_outside);

        sam_itr_destroy(iter);
        bam_hdr_destroy(header);
        bam_destroy1(b);
        hts_idx_destroy(idx);
        sam_close(fp);
    }
}
