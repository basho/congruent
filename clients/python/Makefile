PYVER=`python -V 2>&1 | cut -d' ' -f 2 | cut -d. -f1,2`

all:
	@echo "Using `python -V 2>&1`:"
	@easy_install http://downloads.basho.com/support/protobuf-2.3.0-py${PYVER}.egg
	@easy_install -U riak
