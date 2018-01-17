target=example

build:
	go install  github.com/vc60er/go-qb/example
#	go install ${target}

run: build
	${target} -alsologtostderr -v 10 2>&1 | tee a.log


run1: build
	${target} -alsologtostderr -v 10 2>&1 | tee 1.log

run2: build
	${target} -alsologtostderr -v 10 2>&1 | tee 2.log

run3: build
	${target} -alsologtostderr -v 10 2>&1 | tee 3.log


test:
	go test -v -run Test_QumemeMgr TestHelloWorld qb

watch:
	watch -n 1 -d './cli.sh get --prefix /qb'





