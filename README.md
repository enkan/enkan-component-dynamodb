# enkan-component-dynamodb

## Usage

```java
EnkanSystem system = EnkanSystem.of("dynamoManager", builder(new DynamoDbManager())
        .set(DynamoDbManager::setAccessKey, accessKey)
        .set(DynamoDbManager::setSecretKey, secretKey)
        .build());
```

```java
@Inject
DynamoDbManager dynamoManager;

KeyValuStore store = dynamoManager.createDynamoDbStore();
```
