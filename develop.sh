#!/bin/bash
set -e

function help {
  echo "Usage: ./develop.sh BUILD_DIR --zmq=ZMQ_VERSION --pyzmq=PYZMQ_VERSION"
  exit 1
}

BUILD_DIR=$(readlink -f $1)
shift
for arg in "$@"
do
  case $arg in
    --zmq=*)
      ZMQ_VERSION="${arg#*=}"
      shift;;
    --pyzmq=*)
      PYZMQ_VERSION="${arg#*=}"
      shift;;
    *)
      help;;
  esac
done

if [[ -z "$ZMQ_VERSION" ]]
then
  ZMQ_STRING="the latest zmq"
else
  ZMQ_STRING="zmq-${ZMQ_VERSION}"
fi
if [[ -z "$PYZMQ_VERSION" ]]
then
  PYZMQ_STRING="the latest pyzmq"
else
  PYZMQ_STRING="pyzmq-${PYZMQ_VERSION}"
fi
echo "Installing ${ZMQ_STRING} and ${PYZMQ_STRING} under ${BUILD_DIR}..."

mkdir -p $BUILD_DIR
pushd $BUILD_DIR

### ZeroMQ ####################################################################

if [[ "$ZMQ_VERSION" == 4.1.* ]] || [[ -z "$ZMQ_VERSION" ]]
then
  # zeromq-4.1.x requires libpgm and libsodium.
  sudo apt-get install libpgm-dev
  # ZeroMQ installation fails with libsodium-1.0.6:
  # https://github.com/zeromq/libzmq/issues/1632
  git clone -b 1.0.5 https://github.com/jedisct1/libsodium.git
  pushd libsodium
  ./autogen.sh
  ./configure
  make check
  sudo make install
  sudo ldconfig
  popd
fi
if [[ -z "$ZMQ_VERSION" ]]
then
  # Use the latest version of ZeroMQ if not specified.
  git clone https://github.com/zeromq/libzmq.git
  pushd libzmq
  ./autogen.sh
else
  if [[ "$ZMQ_VERSION" == 4.1.* ]]
  then
    ZMQ_REPO="zeromq4-1"
  elif [[ "$ZMQ_VERSION" == 4.* ]]
  then
    ZMQ_REPO="zeromq4-x"
  elif [[ "$ZMQ_VERSION" == 3.* ]]
  then
    ZMQ_REPO="zeromq3-x"
  elif [[ "$ZMQ_VERSION" == 2.* ]]
  then
    ZMQ_REPO="zeromq2-x"
  fi
  ZMQ_REPO_URL="https://github.com/zeromq/$ZMQ_REPO"
  ZMQ_URL="$ZMQ_REPO_URL/releases/download"
  ZMQ_URL="$ZMQ_URL/v$(sed 's/-.\+//' <<< $ZMQ_VERSION)"
  ZMQ_URL="$ZMQ_URL/zeromq-$ZMQ_VERSION.tar.gz"
  if ! curl -L $ZMQ_URL | tar xz
  then
    # There's no release.  Build from a commit archive.
    ZMQ_URL="$ZMQ_REPO_URL/archive/v$ZMQ_VERSION.tar.gz"
    curl -L $ZMQ_URL | tar xz
    pushd zeromq*
    ./autogen.sh
  else
    # Released officially.
    pushd zeromq-*
  fi
fi
if [[ "$ZMQ_VERSION" == 2.* ]]
then
  # Dependencies of zeromq-2.x.
  sudo apt-get install uuid-dev
fi
sudo apt-get install autoconf libtool
./configure --with-pgm --prefix=$BUILD_DIR/local
make
make install
popd

### PyZMQ #####################################################################

# There was a compiling error with Cython-0.24.
# (http://askubuntu.com/questions/739340/pyzmq-compiling-error)
pip install cython==0.23.5
git clone -b v$PYZMQ_VERSION https://github.com/zeromq/pyzmq.git pyzmq
pushd pyzmq
python setup.py clean
python setup.py configure --zmq=$BUILD_DIR/local
python setup.py build_ext --inplace
pip install -e .
popd

echo "Successfully ${ZMQ_STRING} and ${PYZMQ_STRING} installed."
