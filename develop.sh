#!/bin/bash
set -e

function help {
  echo "Usage: ./develop.sh BUILD_DIR --zmq=ZMQ_VERSION --pyzmq=PYZMQ_VERSION"
  exit 1
}

function info {
  tput bold && echo $@ && tput sgr0
}

[[ -z "$1" ]] && help
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
info "Installing ${ZMQ_STRING} and ${PYZMQ_STRING} under ${BUILD_DIR}..."

mkdir -p $BUILD_DIR
pushd $BUILD_DIR

### ZeroMQ ####################################################################

# Download the source.
if [[ -z "$ZMQ_VERSION" ]]
then
  # Use the latest version of ZeroMQ if not specified.
  ZMQ_DIR="${BUILD_DIR}/libzmq"
  if [[ -d "$ZMQ_DIR" ]]
  then
    pushd libzmq
    git fetch origin
    git checkout origin/master
  else
    git clone https://github.com/zeromq/libzmq.git "$ZMQ_DIR"
    pushd libzmq
  fi
else
  ZMQ_RELEASE=$(sed 's/-.\+//' <<< $ZMQ_VERSION)
  ZMQ_BUILT="${BUILD_DIR}/zeromq-${ZMQ_RELEASE}-built"
  if [[ ! -d $ZMQ_DIR ]]
  then
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
    ZMQ_URL="$ZMQ_URL/v$ZMQ_RELEASE"
    ZMQ_URL="$ZMQ_URL/zeromq-$ZMQ_VERSION.tar.gz"
    if curl -L $ZMQ_URL | tar xz
    then
      ZMQ_DIR="${BUILD_DIR}/zeromq-${ZMQ_RELEASE}"
    else
      # There's no release.  Build from a commit archive.
      ZMQ_URL="$ZMQ_REPO_URL/archive/v$ZMQ_VERSION.tar.gz"
      curl -L $ZMQ_URL | tar xz
      ZMQ_DIR="${BUILD_DIR}/${ZMQ_REPO}-${ZMQ_VERSION}"
    fi
  fi
  pushd $ZMQ_DIR
fi
# Resolve dependencies via APT.  It should be resolved
# even though libzmq already built.
if [[ "$ZMQ_VERSION" == 4.1.* ]] || [[ -z "$ZMQ_VERSION" ]]
then
  sudo apt-get install -y libpgm-dev
elif [[ "$ZMQ_VERSION" == 2.* ]]
then
  sudo apt-get install -y uuid-dev
fi
# Build libzmq.
if [[ -n "$ZMQ_BUILT" ]] && [[ -f "$ZMQ_BUILT" ]]
then
  info "Skipped to build ${ZMQ_STRING} again."
else
  info "Building ${ZMQ_STRING}..."
  [[ -f autogen.sh ]] && ./autogen.sh
  if [[ "$ZMQ_VERSION" == 4.1.* ]] || [[ -z "$ZMQ_VERSION" ]]
  then
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
  sudo apt-get install -y autoconf libtool
  ./configure --with-pgm --prefix=$BUILD_DIR/local
  make
  # Mark as built.
  [[ -n "$ZMQ_BUILT" ]] && touch "$ZMQ_BUILT"
fi
make install
popd

### PyZMQ #####################################################################

info "Building ${PYZMQ_STRING}..."
PYZMQ_DIR="${BUILD_DIR}/pyzmq"
# There was a compiling error with Cython-0.24.
# (http://askubuntu.com/questions/739340/pyzmq-compiling-error)
pip install cython==0.23.5
if [[ -d "$PYZMQ_DIR" ]]
then
  pushd "$PYZMQ_DIR"
  git fetch origin
  git checkout v$PYZMQ_VERSION
else
  git clone -b v$PYZMQ_VERSION https://github.com/zeromq/pyzmq.git "$PYZMQ_DIR"
  pushd "$PYZMQ_DIR"
fi
python setup.py clean
python setup.py configure --zmq="$BUILD_DIR/local"
python setup.py build_ext --inplace
# Uninstall the previous pyzmq clearly.
while pip uninstall -y pyzmq 2>/dev/null; do sleep 0; done
python setup.py install
popd

info "Successfully ${ZMQ_STRING} and ${PYZMQ_STRING} installed."
