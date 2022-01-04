# Contributing to RSS
Any contributions from the open-source community to improve this project are welcome!

## Code Style
This project uses check-style plugins. Run some checks before you create a new pull request.
```shell
mvn clean 
mvn install -DskipTests -Pspark-2
mvn checkstyle:check -Pspark-2
mvn scalastyle:check -Pspark-2
mvn test -Pspark-2
```

## How to Contribute
For collaboration, feel free to contact us. To report a bug, you can just open an issue on GitHub
and attach the exceptions and your analysis if any. For other improvements, you can contact us or
open an issue first and describe what improvement you would like to do. After reaching a consensus,
you can open a pull request and your pull request will get merged after reviewed.

## Improvements on the Schedule
There are already some further improvements on the schedule and welcome to contact us for collaboration:
1. Spark AE Support.
2. Metrics Enhancement.
3. Multiple-Engine Support.
