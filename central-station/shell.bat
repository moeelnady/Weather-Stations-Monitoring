@echo off
mvn package && java -cp ./target/central-station-1.0-SNAPSHOT.jar org.example.shell.BitcaskShell %*
