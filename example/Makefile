target=example

build:
	go install  github.com/vc60er/go-qb/example
#	go install ${target}

run: build
	${target} -alsologtostderr  2>&1 | tee a.log


run1: build
	${target} -alsologtostderr  2>&1 | tee 1.log

run2: build
	${target} -alsologtostderr  2>&1 | tee 2.log

run3: build
	${target} -alsologtostderr  2>&1 | tee 3.log


pub:
	go test -v -run Test_publish_msg  github.com/vc60er/go-qb

watch:
	watch -n 1 -d './cli.sh get --prefix /qb'





