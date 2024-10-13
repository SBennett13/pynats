#!/bin/bash

mkcert -install
mkcert -cert-file server-cert.pem -key-file server-key.pem localhost ::1
mkcert -client -cert-file client-cert.pem -key-file client-key.pem localhost ::1 email@localhost
