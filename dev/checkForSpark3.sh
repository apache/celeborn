mvn clean
mvn install -DskipTests -Pspark-3 -Plog4j-1
mvn checkstyle:check -Pspark-3 -Plog4j-1
mvn scalastyle:check -Pspark-3 -Plog4j-1