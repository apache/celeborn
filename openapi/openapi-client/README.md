# Celeborn OpenAPI Client

To update the OpenAPI specification
- just update the specification under `openapi/openapi-client/src/main/openapi3/` and keep the schema definitions consistent between master and worker.
- Install JDK 11 or above by whatever mechanism is appropriate for your system, and set that version to be the default Java version (e.g., by setting env variable `JAVA_HOME`)
- run the following to regenerate the OpenAPI data models + APIs in the celeborn-openapi-client SDK:
  ```sh
  build/mvn -pl openapi/openapi-client clean package -Pgenerate
  ```
  or
  ```sh
  build/sbt "clean;celeborn-openapi-client/generate"
  ```   
- run the following to check the OpenAPI spec and generated code:
  ```sh
  build/mvn -pl openapi/openapi-client clean package -Pcheck
  ```
  or
  ```sh
  build/sbt "clean;celeborn-openapi-client/check"
  ```
