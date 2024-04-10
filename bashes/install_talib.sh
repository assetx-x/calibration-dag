#!/bin/bash

wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz

tar -xzf ta-lib-0.4.0-src.tar.gz

cd ta-lib/ || echo "ta-lib directory not found!" && exit 1

./configure --prefix=/usr

make

make install

cd .. || echo "ta-lib directory not found!" && exit 1

rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

echo "ta-lib installation complete!"
