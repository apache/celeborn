mvn clean
mvn install -DskipTests -Pspark-2
mvn checkstyle:check -Pspark-2
mvn scalastyle:check -Pspark-2