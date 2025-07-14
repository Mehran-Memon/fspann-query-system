
---

# FSPANN-query-system

## Overview

The `fspann-query-system` is a Java-based implementation of a **Forward-Secure Privacy Preserving Approximate Nearest Neighbor (ANN) Search System** using Locality-Sensitive Hashing (LSH). This project provides a secure and efficient solution for performing ANN queries on high-dimensional data while ensuring forward security through key rotation and encryption. It is designed for applications requiring privacy-preserving similarity search, such as recommendation systems, image retrieval, or nearest neighbor searches in machine learning workflows.

### Key Features

- **Forward Security**: Implements key rotation to ensure past data remains secure even if current keys are compromised.
- **Locality-Sensitive Hashing (LSH)**: Utilizes an EvenLSH algorithm for efficient indexing and querying of high-dimensional data.
- **Encryption**: Encrypts data points using AES to protect sensitive information.
- **Scalability**: Supports concurrent query processing with a thread pool.
- **Modularity**: Organized into packages for indexing, querying, encryption, and key management.

## Project Structure

The project is organized into several packages under `src/main/java/com/fspann/`:

- **`index`**: Contains classes for LSH indexing and bucket management.
    - `EvenLSH.java`: Implements the LSH algorithm for even bucket division.
    - `SecureLSHIndex.java`: Manages the LSH index with support for encryption and forward security.
    - `BucketConstructor.java`: Handles bucket creation, merging, and padding with fake points.
- **`query`**: Manages query generation, processing, and tokenization.
    - `EncryptedPoint.java`: Represents an encrypted data point with metadata.
    - `QueryGenerator.java`: Generates query tokens for ANN searches.
    - `QueryProcessor.java`: Processes queries on the server side.
    - `QueryToken.java`: Encapsulates query metadata, including candidate buckets and encrypted query vectors.
    - `QueryClientHandler.java`: Placeholder for client-side query handling.
- **`encryption`**: Handles encryption and decryption of data.
    - `EncryptionUtils.java`: Provides AES encryption/decryption utilities.
- **`keymanagement`**: Manages encryption keys and key rotation.
    - `KeyManager.java`: Handles key generation, rotation, and epoch tracking.
- **`com.fspann`**: Main system class.
    - `ForwardSecureANNSystem.java`: The core system class that integrates all components for ANN search.

## Prerequisites

To build and run this project, ensure you have the following:

- **Java**: JDK 23 or higher (developed and tested with Java 23.0.2).
- **Maven**: Version 3.9.9 or higher (bundled with IntelliJ IDEA Ultimate or standalone).
- **IntelliJ IDEA**: Optional but recommended for development (used in the project setup).
- **Dependencies**:
    - `slf4j-api`: For logging (version 2.0.16 recommended).
    - Other dependencies are managed via Maven (see `pom.xml`).

## Setup Instructions

### 1. Clone the Repository

Clone the project to your local machine:

```bash
git clone <repository-url>
cd fspann-query-system
```

### 2. Configure Maven

Ensure Maven is installed and configured. If using IntelliJ IDEA, the bundled Maven (3.9.9) should work. Otherwise, download Maven from [https://maven.apache.org/download.cgi](https://maven.apache.org/download.cgi) and set it up:

- Set `MAVEN_HOME` to the Maven installation directory (e.g., `C:\apache-maven-3.9.9`).
- Add `%MAVEN_HOME%\bin` to your `Path` environment variable.

### 3. Resolve Network Issues (If Any)

The project requires downloading dependencies from Maven Central. If you encounter network timeouts (e.g., "Connect timed out" errors), configure a Maven mirror in `~/.m2/settings.xml`:

```xml
<settings>
  <mirrors>
    <mirror>
      <id>alimaven</id>
      <name>Alibaba Cloud Maven Mirror</name>
      <url>https://maven.aliyun.com/repository/public</url>
      <mirrorOf>central</mirrorOf>
    </mirror>
  </mirrors>
</settings>
```

If behind a proxy, add proxy settings to `settings.xml` (replace `<proxy-host>`, `<proxy-port>`, etc., with your proxy details):

```xml

<proxies>
    <proxy>
        <id>my-proxy</id>
        <active>true</active>
        <protocol>http</protocol>
        <host proxy-host="">=""></host>
        <port proxy-port="">
        </port>
        <nonProxyHosts>localhost|127.0.0.1</nonProxyHosts>
    </proxy>
</proxies>
```

### 4. Build the Project

Build the project using Maven:

```bash
cd C:\Users\Mehran Memon\eclipse-workspace\fspann-query-system
mvn clean install
```

If you encounter issues with plugins (e.g., `maven-install-plugin`, `maven-deploy-plugin`, `maven-site-plugin`), ensure they are explicitly declared in `pom.xml`:

```xml
<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-install-plugin</artifactId>
      <version>3.1.1</version>
    </plugin>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-deploy-plugin</artifactId>
      <version>3.1.1</version>
    </plugin>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-site-plugin</artifactId>
      <version>3.12.0</version>
    </plugin>
  </plugins>
</build>
```

### 5. Run the Application

Run the main class `ForwardSecureANNSystem` to test the system:

- **From IntelliJ**:
    - Open `src/main/java/com/fspann/ForwardSecureANNSystem.java`.
    - Right-click and select `Run 'ForwardSecureANNSystem.main()'`.
- **From Command Line**:
    - After building, run:
      ```bash
      java -cp target/fspann-query-system-0.0.1-SNAPSHOT.jar com.fspann.ForwardSecureANNSystem
      ```

#### Expected Output

```
2025-03-13 11:24:48 [main] INFO  com.fspann.ForwardSecureANNSystem - Starting ForwardSecureANNSystem...
2025-03-13 11:24:49 [main] INFO  com.fspann.ForwardSecureANNSystem - Nearest neighbor: [1.0, 1.0, ..., 1.0]
2025-03-13 11:24:49 [main] INFO  com.fspann.ForwardSecureANNSystem - Query executor shut down
2025-03-13 11:24:49 [main] INFO  com.fspann.ForwardSecureANNSystem - ForwardSecureANNSystem shutdown successfully
```

## Usage

### Insert Data

Insert high-dimensional vectors into the system:

```java
ForwardSecureANNSystem system = new ForwardSecureANNSystem(128, 5, 15, 1000, 1500, Collections.emptyList());
double[] vector1 = new double[128];
Arrays.fill(vector1, 1.0);
system.insert("point1", vector1);
```

### Query Nearest Neighbors

Query the system for the nearest neighbors to a given vector:

```java
double[] queryVector = new double[128];
Arrays.fill(queryVector, 1.5);
List<double[]> nearestNeighbors = system.query(queryVector, 1);
System.out.println("Nearest neighbor: " + Arrays.toString(nearestNeighbors.get(0)));
```

### Shutdown

Shut down the system to release resources:

```java
system.shutdown();
```

## Configuration

- **Dimensions**: Set the dimensionality of the vectors (e.g., 128 in the example).
- **Number of Hash Tables**: Controls the number of LSH hash tables (e.g., 5).
- **Number of Intervals**: Defines the number of buckets per hash table (e.g., 15).
- **Bucket Sizes**: `maxBucketSize` and `targetBucketSize` control bucket merging and padding.
- **Key Rotation**: The `KeyManager` rotates keys after a specified number of operations (default: 1000).

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Make your changes and commit (`git commit -m "Add your feature"`).
4. Push to your branch (`git push origin feature/your-feature`).
5. Open a pull request.

## Troubleshooting

- **Network Timeouts**: If Maven fails to download dependencies, configure a mirror (e.g., Alibaba Cloud) in `settings.xml`.
- **Java Version Issues**: Ensure you’re using Java 23 or higher.
- **Build Failures**: Run `mvn clean install -e -X` for detailed debug logs and share them for assistance.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

For questions or issues, contact the project maintainers at [mehran@stu.xidian.edu.cn].

---