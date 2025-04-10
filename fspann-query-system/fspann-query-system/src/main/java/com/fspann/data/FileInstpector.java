package java.com.fspann.data;

import java.io.DataInputStream;
import java.io.FileInputStream;

public class FileInstpector {
    public static void main(String[] args) throws Exception {
        String filePath = "C:\\Users\\Mehran Memon\\eclipse-workspace\\fspann-query-system\\fspann-query-system\\data\\sift_dataset\\sift\\sift_base.fvecs";
        try (DataInputStream dis = new DataInputStream(new FileInputStream(filePath))) {
            int dim = dis.readInt();
            System.out.println("First dimension: " + dim);
        }
    }
}
