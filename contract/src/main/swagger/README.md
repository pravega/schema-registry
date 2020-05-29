<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
Instructions to generate Server REST API stubs

## Delete previously generated directory
```
rm -Rf server/src/main/java/io/pravega/schemaregistry/server/io.pravega.rest/generated
```

## Update schemaregistry.yaml
All REST API modifications should be done by updating the swagger/schemaregistry.yaml specification file.
This can be done manually or by using the online editor at http://editor.swagger.io.

## Download Swagger codegen
Download swagger-codegen-cli from maven - http://repo1.maven.org/maven2/io/swagger/swagger-codegen-cli/2.2.3/swagger-codegen-cli-2.2.3.jar

## Generate the API stubs using Swagger Codegen
```
java -jar swagger-codegen-cli.jar generate -i <schema registry root>/contract/src/main/swagger/SchemaRegistry.yaml -l jaxrs -c <schema registry root>/contract/src/main/swagger/server.config.json -o <schema registry root>/contract/
```

## Remove extra files created by codegen
All files that get generated outside of the contract/src/main/java/io/pravega/schemaregistry/contract/generated/rest folder should be deleted and not committed to git.

## Update ApiV1.java
The JAXRS API stubs decorated with swagger annotations are generated in .../contract/io/pravega/schemaregistry/contract/v1/ApiV1.java class.
Copy these API descriptions into interfaces in .../contract/io.pravega.schemaregistry/contract//v1/ApiV1.java. 
Also make an asynchronous version of APIs in .../contract/io.pravega.schemaregistry/server/rest/v1/ApiV1.java to use only jersey async interfaces.

## Generate documentation
### Download Swagger2Markup CLI
https://jcenter.bintray.com/io/github/swagger2markup/swagger2markup-cli/1.3.3/swagger2markup-cli-1.3.3.jar

### Generate and save the markup documentation
```
java -Dswagger2markup.markupLanguage=MARKDOWN -Dswagger2markup.generatedExamplesEnabled=true -jar swagger2markup-cli-1.3.3.jar  convert -i <root>/contract/src/main/swagger/SchemaRegistry.yaml -f <root>/documentation/src/docs/io.pravega.rest/restapis
```
