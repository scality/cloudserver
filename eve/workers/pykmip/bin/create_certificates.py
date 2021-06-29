#!/usr/bin/env python

from cryptography import x509
from cryptography.hazmat import backends
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import datetime
import argparse
import sys


def get_args():
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Tool to generate a x509 CA root, server and client certs')
    parser.add_argument('-c', '--common-name', action='store',
                        default='localhost',
                        help='Set the common name for the server-side cert')
    return parser.parse_args()


def create_rsa_private_key(key_size=2048, public_exponent=65537):
    private_key = rsa.generate_private_key(
        public_exponent=public_exponent,
        key_size=key_size,
        backend=backends.default_backend()
    )
    return private_key


def create_self_signed_certificate(subject_name,
                                   private_key,
                                   days_valid=36500):
    subject = x509.Name([
        x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, u"Scality"),
        x509.NameAttribute(x509.NameOID.COMMON_NAME, subject_name)
    ])
    certificate = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        subject
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=days_valid)
    ).add_extension(
        x509.BasicConstraints(True, None),
        critical=True
    ).sign(private_key, hashes.SHA256(), backends.default_backend())

    return certificate


def create_certificate(subject_name,
                       private_key,
                       signing_certificate,
                       signing_key,
                       days_valid=36500,
                       client_auth=False):
    subject = x509.Name([
        x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, u"Scality"),
        x509.NameAttribute(x509.NameOID.COMMON_NAME, subject_name)
    ])
    builder = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        signing_certificate.subject
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=days_valid)
    )

    if client_auth:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([x509.ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=True
        )

    certificate = builder.sign(
        signing_key,
        hashes.SHA256(),
        backends.default_backend()
    )
    return certificate


def main(common_name):
    root_key = create_rsa_private_key()
    root_certificate = create_self_signed_certificate(
        u"Root CA",
        root_key
    )

    server_key = create_rsa_private_key()
    server_certificate = create_certificate(
        common_name,
        server_key,
        root_certificate,
        root_key
    )

    john_doe_client_key = create_rsa_private_key()
    john_doe_client_certificate = create_certificate(
        u"John Doe",
        john_doe_client_key,
        root_certificate,
        root_key,
        client_auth=True
    )

    with open("certs/kmip-ca.pem", "wb") as f:
        f.write(
            root_certificate.public_bytes(
                serialization.Encoding.PEM
            )
        )
    with open("certs/kmip-key.pem", "wb") as f:
        f.write(server_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    with open("certs/kmip-cert.pem", "wb") as f:
        f.write(
            server_certificate.public_bytes(
                serialization.Encoding.PEM
            )
        )
    with open("certs/kmip-client-key.pem", "wb") as f:
        f.write(john_doe_client_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    with open("certs/kmip-client-cert.pem", "wb") as f:
        f.write(
            john_doe_client_certificate.public_bytes(
                serialization.Encoding.PEM
            )
        )


if __name__ == '__main__':
    args = get_args()
    main(args.common_name)
