#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple utility to (re-)generate the files within resources/ssl
# These generated certificates are used for a variety of test scenarios for SSL.
# The utility ends up generating two certificates - which are saved into two different keystores
# The certificates generated are signed by two different CA cert's (also generated here).
# There are two truststores generated - the first truststore has both CA certs as part of it
# Hence this trust can be used to validate both client certificates.
# The second trust store has NO CA certs in it - and so when used will fail both the certificates.
# Requires: "openssl" (typically the openssl package) and java "keytool" in the PATH

function gen_certs() {

  openssl genrsa -out ca.key 2048
  openssl req -x509 -new -days 9000 -key ca.key -out ca.crt -subj "/C=US/ST=YourState/L=YourCity/O=YourOrganization/CN=MyCACert"
  openssl genrsa -out server.key 2048
  openssl req -new -key server.key -out server.csr -subj "/C=US/ST=YourState/L=YourCity/O=YourOrganization/CN=MyServer"
  openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial -in server.csr -out server.crt -days 8000
  openssl pkcs12 -export -in server.crt -inkey server.key -out keystore.p12 -name servercert -password pass:password
  keytool -importkeystore -destkeystore server.jks -srckeystore keystore.p12 -srcstoretype PKCS12 -deststoretype pkcs12 -srcstorepass password -deststorepass password -noprompt

  keytool -import -trustcacerts -alias CACert -file ca.crt -keystore truststore.jks -storepass password -noprompt

  rm ca.srl keystore.p12 server.csr ca.key server.key server.crt
}


mkdir for_default
cd for_default
gen_certs
cd ..
mkdir for_secondary
cd for_secondary
gen_certs
cd ..


cp ./for_default/truststore.jks ./for_default/server.jks .
cp ./for_secondary/server.jks ./server_another.jks


keytool -import -trustcacerts -alias 'CACertAnother' -file for_secondary/ca.crt -keystore ./truststore.jks -storepass password -noprompt

# Copy the secondary trust store and remove the ca to generate truststore-without-ca.jks
cp ./for_secondary/truststore.jks ./truststore-without-ca.jks
keytool -delete -alias 'CACert' -keystore ./truststore-without-ca.jks -storepass password -noprompt

rm -rf for_default for_secondary
