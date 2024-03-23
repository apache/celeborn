#!/bin/bash


function gen_certs() {

  openssl genrsa -out ca.key 2048
  openssl req -x509 -new -days 9000 -key ca.key -out ca.crt   -subj "/C=US/ST=YourState/L=YourCity/O=YourOrganization/CN=MyCACert"
  openssl genrsa -out server.key 2048
  openssl req -new -key server.key -out server.csr   -subj "/C=US/ST=YourState/L=YourCity/O=YourOrganization/CN=MyServer"
  openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial -in server.csr -out server.crt -days 8000
  openssl pkcs12 -export -in server.crt -inkey server.key -out keystore.p12 -name servercert -password pass:password
  keytool -importkeystore -destkeystore server.jks -srckeystore keystore.p12 -srcstoretype PKCS12 -deststoretype pkcs12 -srcstorepass password  -deststorepass password -noprompt

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
