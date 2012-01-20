CLIENTS=$(shell ls clients)

.PHONY: deps clients

all: clients deps compile

deps:
	./rebar get-deps

compile:
	./rebar compile

clients: $(CLIENTS)

$(CLIENTS):
	make -C clients/$@
