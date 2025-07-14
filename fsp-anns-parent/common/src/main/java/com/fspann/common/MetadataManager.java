package com.fspann.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MetadataManager {

    void putVectorMetadata(String vectorId, Map<String, String> metadataMap);

    Map<String, String> getVectorMetadata(String vectorId);

    void updateVectorMetadata(String vectorId, Map<String, String> updates);

    List<EncryptedPoint> getAllEncryptedPoints();

    void saveEncryptedPoint(EncryptedPoint point) throws IOException;


    void shutdown();
}
