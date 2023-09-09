## Uservice REST Template

A generic template for a REST based microservice based on [guardrail](https://guardrail.dev) 

### To compile and generate APIs

```bash
brew install sbt
```
 
- On Preferences/Code Style/Scala 
  - choose "Scalafmt" as Formatter
  - check the "Reformat on file save" option

### Run the app and launching tests

```bash
sbt             # to enter the sbt console
compile
run
```

### API UI from browser
- When the app is running use the following link to access the API [swagger](http://127.0.0.1:8093/uservice-rest-template/0.0/swagger-ui/index.html)