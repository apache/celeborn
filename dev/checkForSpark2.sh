mvn clean compile  -Pspark-2 -Plog4j-1
mvn install -DskipTests -Pspark-2 -Plog4j-1
mvn checkstyle:check -Pspark-2 -Plog4j-1
mvn scalastyle:check -Pspark-2 -Plog4j-1
