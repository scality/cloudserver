#!/bin/bash
npm install
# No need to download s3curl.pl since include it in the directory.
# By including it, we are able to preset the accessKey and secretKey.
# We are also able to fix the script so can be used by non-English
# speakers (otherwise dates in other languages cause errors).
# So, just need to make it executable and get dependencies.
chmod ugoa+x ./s3curl.pl
curl -L http://cpanmin.us | perl - --sudo App::cpanminus
sudo cpanm YAML HTTP::Date Digest::HMAC_SHA1 URI::Escape
