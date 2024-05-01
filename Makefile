MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MKFILE_DIR := $(dir $(MKFILE_PATH))
#GOPATH := $(abspath $(MKFILE_PATH)/..)
#REV := $(shell git rev-parse --short HEAD)

all:
	# go mod init github.com/vderic/kite-client-go
	# go mod tidy
	go get .
	go build .


clean:
	go clean .


.PHONY: all install clean
