all test check:
	mkdir -p build
	$(MAKE) build/http_tests
	LD_LIBRARY_PATH=../.. build/http_tests # FIXME

HTTP_TESTS_OBJS=build/http_tests.o build/test_httpd.o build/curlstream.o build/curlstream_tests.o
build/http_tests: build/catch.hpp $(HTTP_TESTS_OBJS)
	g++ -o $@ -g -pthread \
		-L../.. \
		$(HTTP_TESTS_OBJS) -lhts -lmicrohttpd -lcurl

build/%.o: %.cc
	g++ -c -g -Wall -std=c++11 \
		-I../.. \
		-o $@ $^

build/catch.hpp: build/catch.hpp
	cd build && wget https://raw.github.com/philsquared/Catch/master/single_include/catch.hpp

clean:
	rm -rf build

.PHONY: all test rocksdb-on-cloud clean
