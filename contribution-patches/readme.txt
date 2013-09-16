GORA-229: Use @Ignore for unimplemented test functionality
Jira issue URL: https://issues.apache.org/jira/browse/GORA-229
This issue is about the use of JUnit 4 features in the test cases, which is essentially an
improvement for the testing suite of the project. More specifically, it involves refactoring all
the Java classes of Apache Gora to abandon the deprecated junit.framework.* and use the
recommended static org.junit.Assert.*. Additionally, it involves making use of the @Ignore
annotation for the test methods that do not have proper implementation yet and should
be skipped. This was a very big patch that affected all the modules and data stores of the
project. The patch has already been committed to the project.

GORA-231: Provide better error handling in AccumuloStore.readMapping
Jira issue URL: https://issues.apache.org/jira/browse/GORA-231
Each Gora datastore has to read the mapping file and parse it accordingly. However, there
is no check and proper error handling in case the mapping file does not exist. The goal was
to create a patch that would provide a consistent error handling among all Gora datastore.
For this reason I modified the DataStoreFactory.getMappingFile() which is used by all datastores.
It now throws an IOException if the mapping file does not exist in the path. The
patch was submitted but has not yet been committed at the time of writing this report.

GORA-232: DataStoreTestBase functionality delegation to DataStoreTestUtil
Jira issue URL: https://issues.apache.org/jira/browse/GORA-232
This issue was an improvement of the implementation of the testing suite. The pattern that
the test suite follows demands that alDataStoreTestUtil should be responsible for the actual
testing. However, some test cases defined in DataStoreTestBase contained an actual test
code implementation. The patch that I contributed delegated all the testing functionality
to DataStoreTestUtil, where appropriate. The patch has already been committed to the
project.

GORA-243: Properly escaping spaces of GORA_HOME in bin/gora
Jira issue URL: https://issues.apache.org/jira/browse/GORA-243
This bug was identified by me. The problem was that when the user executed “bin/gora”
from a path that contained one or more spaces, it failed with the following exception:
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/Avro/Schema
The cause was the mishandling of spaces when building the CLASSPATH environmental
variable in bin/gora. The solution was to save the IFS (Internal Field Separator) variable
into a temporary variable, the build the CLASSPATH environmental variable and then
restore the original value of IFS from the temporary variable. The patch has already been
committed to the project.

GORA-258: writeCapacUnits gets value from wrong attribute
Jira issue URL: https://issues.apache.org/jira/browse/GORA-258
This bug was identified me and involves a variable that reads a value from a wrong property
in the Gora-DynamoDB module. More specifically, I spotted that the variable writeCapacUnits
was wrongly reading its value from the element readCapacUnits during the xml
parsing of the mapping file. The patch has already been accepted and committed.

GORA-259: Removal of the main methods from the test case classes
Jira issue URL: https://issues.apache.org/jira/browse/GORA-259
This improvement was identified by me. I spotted that several JUnit test case classes
contained main methods for triggering some test cases, which is a non-recommended practice.
After I received positive feedback from the community, I proceeded to create the patch, which
has already been committed.

GORA-264: Make generated data beans more java doc friendly
Jira issue URL: https://issues.apache.org/jira/browse/GORA-264
This issue is an improvement of the GoraCompiler. The goal of this improvement is to make
the GoraCompiler add static and dynamic javadocs to the generated data beans. In order to
make the javadocs as dynamic as possible, I extended the compiler’s parsing capabilities of
the json Avro schemas. More specifically, the GoraCompiler now extracts information from
the “doc” element of the Avro schema in order to dynamically construct the javadoc for the
generated data bean. This way, the generated data beans provide helpful comments and are
much more readable and maintainable. A sample data bean generated with this patch can
be seen in 2.3.2. The patch has already been accepted and committed.

GORA-265: Support for dynamic file extensions when traversing a directory
Jira issue URL: https://issues.apache.org/jira/browse/GORA-265
Until version 0.3 of Gora, the GoraCompiler had 2 ways of finding the Avro schemes that
it would compile. The first one was to directly specify the filename of the Avro scheme
and the second one was to to specify a directory. In the later case the GoraCompiler would
compile all the Avro schemes whose filenames have the default extension of .avsc. I proposed
this improvement that allows the user to specify dynamically the extensions of the files of a
specific directory. This way the user has much greater flexibility when needing to compile
multiple Avro schemes. The patch has already been accepted and committed.

GORA-268: Make GoraCompiler the main manifest attribute in gora-core
Jira issue URL: https://issues.apache.org/jira/browse/GORA-268
Normally, the GoraCompiler should be invoked by using the bin/gora executable. However,
the GoraCompiler being a part of the gora-core module could be invoked by the mavengenerated
jar file. This functionality was not implemented yet. I submitted this patch that
makes the GoraCompiler the mainClass in the jar’s manifest. Therefore, a user is able now
to simply invoke the GoraCompiler directly from java, as follows:
java -jar gora-core/target/gora-core-0.4-SNAPSHOT.jar.
This provides greater flexibility in invoking the GoraCompiler and also allows the GoraCompiler
to be uncoupled from the rest of the framework.