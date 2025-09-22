import java.nio.file.*;
import java.io.*;

public class ValidateDataset {
    public static void main(String[] args) throws IOException {
        Path baseFile = Paths.get("E:/Research Work/Datasets/synthetic_data/synthetic_128/base.fvecs");
        DataInputStream dis = new DataInputStream(Files.newInputStream(baseFile));
        int numVectors = 1000000; // Number of vectors in the dataset
        int dimension = 128; // Vector dimension

        for (int i = 0; i < numVectors; i++) {
            dis.readInt(); // Read dimension (if applicable)
            for (int j = 0; j < dimension; j++) {
                float value = dis.readFloat();
                if (Float.isNaN(value) || Float.isInfinite(value)) {
                    System.err.println("Invalid value detected at vector " + i + " index " + j + ": " + value);
                }
            }
        }
        dis.close();
    }
}
