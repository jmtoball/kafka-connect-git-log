# kafka-connect-git-log

## What it does

- It takes a list of paths, each containing a git working copy
- It retrieves latest master (TODO: any branch) of the corresponding repo regularly
- It publishes messages containing the new commit data to a given topic

## Usage

- `mvn clean && mvn package`
- copy _kafka-connect-git-log-1.0-SNAPSHOT-jar-with-dependencies.jar_ to kafka-connect plugins folder

## Notes
- This is work in progress and currently just a POC for an application I have. It may or may not become production ready at some point
- It hard-resets the working copy to the origin version each time, so ironically working on the working copy is not possible

## TODO

- Tests
- Publish all/a configurable subset of commit data
- Optionally publish changes too
- Authenticated git access
- Just watch working copies, don't reset them to the latest origin version
- Extract git-related classes