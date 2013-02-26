Information retrieval system for class project:

Installation:

1.  Installation is easy with Maven
2.  Go to the root directory of the project, type the following command

    ```mvn clean install```

3.  Maven will resolve all dependencies.
4.  Edit the following line in yairs/pom.xml (the pom.xml in main directory) to give the configuration file path as argument

  ```
 <mainClass>yairs.retrieval.BooleanRetriever</mainClass>
                        <args>
                            <arg>../conf/boolean.properties</arg>
                        </args>
                        <jvmArgs>
                            <jvmArg>-Xmx512m</jvmArg>
                        </jvmArgs>
  ```

5.  Edit the configuration file, one example was given in conf/boolean.properties,read the comments there for details

6.  To run the program, go to the "core" directory and use he maven launcher

  ```mvn scala:run -Dlauncher="boolean" ```

7.  Alternatively, you can also run the program in your IDE, if under IntelliJ, you can edit the run configuration to pass in the configuration file path

8. You can play around other options by looking at the main class of "core/src/main/scala/retrieval/BooleanRetriever.scala"