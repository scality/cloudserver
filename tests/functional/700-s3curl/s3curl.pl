#!/usr/bin/perl -w

# Copyright 2006-2010 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this
# file except in compliance with the License. A copy of the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
# for the specific language governing permissions and limitations under the License.

use strict;
use POSIX;

# you might need to use CPAN to get these modules.
# run perl -MCPAN -e "install <module>" to get them.

use Digest::HMAC_SHA1;
use Digest::MD5;
use FindBin;
use MIME::Base64 qw(encode_base64);
use Getopt::Long qw(GetOptions);

use constant STAT_MODE => 2;
use constant STAT_UID => 4;

my $ipAddress = $ENV{'IP'};

# begin customizing here
my @endpoints = ( '127.0.0.1',
                  $ipAddress,
                  's3.amazonaws.com',
                  's3-us-west-1.amazonaws.com',
                  's3-us-west-2.amazonaws.com',
                  's3-us-gov-west-1.amazonaws.com',
                  's3-eu-west-1.amazonaws.com',
                  's3-ap-southeast-1.amazonaws.com',
                  's3-ap-northeast-1.amazonaws.com',
                  's3-sa-east-1.amazonaws.com', );

my $CURL = "curl";

# stop customizing here

my $cmdLineSecretKey;
# my %awsSecretAccessKeys = ( # corporate account
#     company => {
#         id => '0KBZMNBYNG7B2XSCYNR2',
#         key => '7wHWfZWxJRJRUFoWJk10d1uRnsxpEvRM87cprujo',
#     });
my %awsSecretAccessKeys = ( # corporate account
    company => {
        id => 'accessKey1',
        key => 'verySecretKey1',
    });
my $keyFriendlyName;
my $keyId='company';
my $secretKey;
my $contentType = "";
my $acl;
my $contentMD5 = "";
my $fileToPut;
my $createBucket;
my $doDelete;
my $doHead;
my $help;
my $debug = 0;
my $copySourceObject;
my $copySourceRange;
my $postBody;
my $calculateContentMD5 = 0;

my $DOTFILENAME=".s3curl";
my $EXECFILE=$FindBin::Bin;
my $LOCALDOTFILE = $EXECFILE . "/" . $DOTFILENAME;
my $HOMEDOTFILE = $ENV{HOME} . "/" . $DOTFILENAME;
my $DOTFILE = -f $LOCALDOTFILE? $LOCALDOTFILE : $HOMEDOTFILE;

if (-f $DOTFILE) {
    open(CONFIG, $DOTFILE) || die "can't open $DOTFILE: $!";

    my @stats = stat(*CONFIG);

    if (($stats[STAT_UID] != $<) || $stats[STAT_MODE] & 066) {
        die "I refuse to read your credentials from $DOTFILE as this file is " .
            "readable by, writable by or owned by someone else. Try " .
            "chmod 600 $DOTFILE";
    }

    my @lines = <CONFIG>;
    close CONFIG;
    eval("@lines");
    die "Failed to eval() file $DOTFILE:\n$@\n" if ($@);
}

GetOptions(
    'id=s' => \$keyId,
    'key=s' => \$cmdLineSecretKey,
    'contentType=s' => \$contentType,
    'acl=s' => \$acl,
    'contentMd5=s' => \$contentMD5,
    'put=s' => \$fileToPut,
    'copySrc=s' => \$copySourceObject,
    'copySrcRange=s' => \$copySourceRange,
    'post:s' => \$postBody,
    'delete' => \$doDelete,
    'createBucket:s' => \$createBucket,
    'head' => \$doHead,
    'help' => \$help,
    'debug' => \$debug,
    'calculateContentMd5' => \$calculateContentMD5,
);

my $usage = <<USAGE;
Usage $0 --id friendly-name (or AWSAccessKeyId) [options] -- [curl-options] [URL]
 options:
  --key SecretAccessKey       id/key are AWSAcessKeyId and Secret (unsafe)
  --contentType text/plain    set content-type header
  --acl public-read           use a 'canned' ACL (x-amz-acl header)
  --contentMd5 content_md5    add Content-MD5 header
  --calculateContentMd5       calculate Content-MD5 and add it
  --put <filename>            PUT request (from the provided local file)
  --post [<filename>]         POST request (optional local file)
  --copySrc bucket/key        Copy from this source key
  --copySrcRange {startIndex}-{endIndex}
  --createBucket [<region>]   create-bucket with optional location constraint
  --head                      HEAD request
  --debug                     enable debug logging
 common curl options:
  -H 'x-amz-acl: public-read' another way of using canned ACLs
  -v                          verbose logging
USAGE
die $usage if $help || !defined $keyId;

if ($cmdLineSecretKey) {
    printCmdlineSecretWarning();
    sleep 5;

    $secretKey = $cmdLineSecretKey;
} else {
    my $keyinfo = $awsSecretAccessKeys{$keyId};
    die "I don't know about key with friendly name $keyId. " .
        "Do you need to set it up in $DOTFILE?"
        unless defined $keyinfo;

    $keyId = $keyinfo->{id};
    $secretKey = $keyinfo->{key};
}

if ($contentMD5 && $calculateContentMD5) {
    die "cannot specify both --contentMd5 and --calculateContentMd5";
}


my $method = "";
if (defined $fileToPut or defined $createBucket or defined $copySourceObject) {
    $method = "PUT";
} elsif (defined $doDelete) {
    $method = "DELETE";
} elsif (defined $doHead) {
    $method = "HEAD";
} elsif (defined $postBody) {
    $method = "POST";
} else {
    $method = "GET";
}
my $resource;
my $host;

if ($calculateContentMD5) {
    if ($fileToPut) {
        $contentMD5 = calculateFileContentMD5($fileToPut);
    } elsif ($createBucket) {
        $contentMD5 = calculateStringContentMD5(getCreateBucketData($createBucket));
    } elsif ($postBody) {
        $contentMD5 = calculateFileContentMD5($postBody);
    } else {
        $contentMD5 = calculateStringContentMD5('');
    }
}

my %xamzHeaders;
$xamzHeaders{'x-amz-acl'}=$acl if (defined $acl);
$xamzHeaders{'x-amz-copy-source'}=$copySourceObject if (defined $copySourceObject);
$xamzHeaders{'x-amz-copy-source-range'}="bytes=$copySourceRange" if (defined $copySourceRange);

# try to understand curl args
for (my $i=0; $i<@ARGV; $i++) {
    my $arg = $ARGV[$i];
    # resource name
    if ($arg =~ /https?:\/\/([^\/:?]+)(?::(\d+))?([^?]*)(?:\?(\S+))?/) {
        $host = $1 if !$host;
        my $port = defined $2 ? $2 : "";
        my $requestURI = $3;
        my $query = defined $4 ? $4 : "";
        debug("Found the url: host=$host; port=$port; uri=$requestURI; query=$query;");
        if (length $requestURI) {
            $resource = $requestURI;
        } else {
            $resource = "/";
        }
        my @attributes = ();
        for my $attribute ("acl", "delete", "location", "logging", "notification",
            "partNumber", "policy", "requestPayment", "response-cache-control",
            "response-content-disposition", "response-content-encoding", "response-content-language",
            "response-content-type", "response-expires", "torrent",
            "uploadId", "uploads", "versionId", "versioning", "versions", "website", "lifecycle") {
            if ($query =~ /(?:^|&)($attribute(?:=[^&]*)?)(?:&|$)/) {
                push @attributes, uri_unescape($1);
            }
        }
        if (@attributes) {
            $resource .= "?" . join("&", @attributes);
        }
        # handle virtual hosted requests
        getResourceToSign($host, \$resource);
    }
    elsif ($arg =~ /\-X/) {
        # mainly for DELETE
    $method = $ARGV[++$i];
    }
    elsif ($arg =~ /\-H/) {
    my $header = $ARGV[++$i];
        #check for host: and x-amz*
        if ($header =~ /^[Hh][Oo][Ss][Tt]:(.+)$/) {
            $host = $1;
        }
        elsif ($header =~ /^([Xx]-[Aa][Mm][Zz]-.+): *(.+)$/) {
            my $name = lc $1;
            my $value = $2;
            # merge with existing values
            if (exists $xamzHeaders{$name}) {
                $value = $xamzHeaders{$name} . "," . $value;
            }
            $xamzHeaders{$name} = $value;
        }
    }
}

die "Couldn't find resource by digging through your curl command line args!"
    unless defined $resource;

my $xamzHeadersToSign = "";
foreach (sort (keys %xamzHeaders)) {
    my $headerValue = $xamzHeaders{$_};
    $xamzHeadersToSign .= "$_:$headerValue\n";
}

# Modified to work for days of the week and months not in English:
my @wd = ("0", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
my $wn = $wd[strftime("%u", gmtime )];
my @month = ("0", "Jan", "Feb", "March", "April", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec");
my $month = @month[strftime("%m", gmtime)];
my $day = POSIX::strftime(", %d ", gmtime);
my $httpDate = POSIX::strftime(" %Y %H:%M:%S +0000", gmtime );
$httpDate = $wn . $day . $month . $httpDate;

# Premodified version of httpDate:
# my $httpDate = POSIX::strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime );

my $stringToSign = "$method\n$contentMD5\n$contentType\n$httpDate\n$xamzHeadersToSign$resource";

debug("StringToSign='" . $stringToSign . "'");
my $hmac = Digest::HMAC_SHA1->new($secretKey);
$hmac->add($stringToSign);
my $signature = encode_base64($hmac->digest, "");


my @args = ();
push @args, ("-H", "Date: $httpDate");
push @args, ("-H", "Authorization: AWS $keyId:$signature");
push @args, ("-H", "x-amz-acl: $acl") if (defined $acl);
push @args, ("-L");
push @args, ("-H", "content-type: $contentType") if (defined $contentType);
push @args, ("-H", "Content-MD5: $contentMD5") if (length $contentMD5);
push @args, ("-T", $fileToPut) if (defined $fileToPut);
push @args, ("-X", "DELETE") if (defined $doDelete);
push @args, ("-X", "POST") if(defined $postBody);
push @args, ("-I") if (defined $doHead);

if (defined $createBucket) {
    # createBucket is a special kind of put from stdin. Reason being, curl mangles the Request-URI
    # to include the local filename when you use -T and it decides there is no remote filename (bucket PUT)
    my $data = getCreateBucketData($createBucket);
    push @args, ("--data-binary", $data);
    push @args, ("-X", "PUT");
} elsif (defined $copySourceObject) {
    # copy operation is a special kind of PUT operation where the resource to put
    # is specified in the header
    push @args, ("-X", "PUT");
    push @args, ("-H", "x-amz-copy-source: $copySourceObject");
} elsif (defined $postBody) {
    if (length($postBody)>0) {
        push @args, ("-T", $postBody);
    }
}

push @args, @ARGV;

debug("exec $CURL " . join (" ", @args));
exec($CURL, @args)  or die "can't exec program: $!";

sub debug {
    my ($str) = @_;
    $str =~ s/\n/\\n/g;
    print STDERR "s3curl: $str\n" if ($debug);
}

sub getResourceToSign {
    my ($host, $resourceToSignRef) = @_;
    for my $ep (@endpoints) {
        if ($host =~ /(.*)\.$ep/) { # vanity subdomain case
            my $vanityBucket = $1;
            $$resourceToSignRef = "/$vanityBucket".$$resourceToSignRef;
            debug("vanity endpoint signing case");
            return;
        }
        elsif ($host eq $ep) {
            debug("ordinary endpoint signing case");
            return;
        }
    }
    # cname case
    $$resourceToSignRef = "/$host".$$resourceToSignRef;
    debug("cname endpoint signing case");
}


sub printCmdlineSecretWarning {
    print STDERR <<END_WARNING;
WARNING: It isn't safe to put your AWS secret access key on the
command line!  The recommended key management system is to store
your AWS secret access keys in a file owned by, and only readable
by you.
For example:
\%awsSecretAccessKeys = (
    # personal account
    personal => {
        id => '1ME55KNV6SBTR7EXG0R2',
        key => 'zyMrlZUKeG9UcYpwzlPko/+Ciu0K2co0duRM3fhi',
    },
    # corporate account
    company => {
        id => '1ATXQ3HHA59CYF1CVS02',
        key => 'WQY4SrSS95pJUT95V6zWea01gBKBCL6PI0cdxeH8',
    },
);
\$ chmod 600 $DOTFILE
Will sleep and continue despite this problem.
Please set up $DOTFILE for future requests.
END_WARNING
}

sub uri_unescape {
  my ($input) = @_;
  $input =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg;
  debug("replaced string: " . $input);
  return ($input);
}

# generate the XML for bucket creation.
sub getCreateBucketData {
    my ($createBucket) = @_;

    my $data = "";
    if (length($createBucket) > 0) {
        $data = "<CreateBucketConfiguration><LocationConstraint>$createBucket</LocationConstraint></CreateBucketConfiguration>";
    }
    return $data;
}

# calculates the MD5 header for a string.
sub calculateStringContentMD5 {
    my ($string) = @_;
    my $md5 = Digest::MD5->new;
    $md5->add($string);
    my $b64 = encode_base64($md5->digest);
    chomp($b64);
    return $b64;
}

# calculates the MD5 header for a file.
sub calculateFileContentMD5 {
    my ($file_name) = @_;
    open(FILE, "<$file_name") || die "could not open file $file_name for MD5 calculation";
    binmode(FILE) || die "could not set file reading to binary mode: $!";
    my $md5 = Digest::MD5->new;
    $md5->addfile(*FILE);
    close(FILE) || die "could not close $file_name";
    my $b64 = encode_base64($md5->digest);
    chomp($b64);
    return $b64;
}
