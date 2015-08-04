.PHONY: all compile upgrade-deps clean distclean test check_plt build_plt dialyzer \
	    cleanplt

all: compile

compile:
	./rebar3 compile

upgrade-deps:
	./rebar3 upgrade

clean:
	./rebar3 clean

distclean:
	./rebar3 clean --all

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto \
				common_test

include tools.mk
