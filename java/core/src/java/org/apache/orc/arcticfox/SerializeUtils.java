package org.apache.orc.arcticfox;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.orc.OrcProto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class SerializeUtils {

    public static ByteBuffer serializeStripeFooter(OrcProto.StripeFooter stripeFooter) {
        byte[] stripeFooterBytes = stripeFooter.toByteArray();
        return ByteBuffer.wrap(stripeFooterBytes);
    }

    public static OrcProto.StripeFooter deserializeStripeFooter(ByteBuffer buffer)
            throws InvalidProtocolBufferException {
        return OrcProto.StripeFooter.parseFrom(buffer.array());
    }

    public static ByteBuffer serializeRowIndex(OrcProto.RowIndex[] rowGroupIndex) {
        int rowIndexCount = rowGroupIndex.length;
        int[] rowIndexLengthList = new int[rowGroupIndex.length];
        int index = 0;
        for(OrcProto.RowIndex rowIndex: rowGroupIndex) {
            rowIndexLengthList[index++] = rowIndex.getSerializedSize();
        }

        int bufferLength = Arrays.stream(rowIndexLengthList).sum() + rowIndexCount * Integer.BYTES;
        ByteBuffer result = ByteBuffer.allocate(bufferLength);

        // store length for each RowIndex
        for(int rowIndexLength: rowIndexLengthList) {
            byte[] tmp = intToByteArray(rowIndexLength);
            result.put(tmp, 0, tmp.length);
        }

        // store RowIndex data
        for(OrcProto.RowIndex rowIndex: rowGroupIndex) {
            byte[] tmp = rowIndex.toByteArray();
            result.put(tmp, 0, tmp.length);
        }
        result.flip();
        return result;
    }

    public static OrcProto.RowIndex[] deserializeRowIndex(ByteBuffer byteBuffer,
                                                          int rowIndexCount) {
        if(rowIndexCount == 0 || byteBuffer == null || byteBuffer.array().length == 0) {
            return null;
        }

        byte[] bytes = byteBuffer.array();
        OrcProto.RowIndex[] rowGroupIndex = new OrcProto.RowIndex[rowIndexCount];
        int[] rowIndexLengthList = new int[rowIndexCount];

        for(int i = 0; i < rowIndexCount; i++) {
            rowIndexLengthList[i]  = byteArrayToInt(bytes, i * Integer.BYTES);
        }

        int currentOffset = rowIndexCount * Integer.BYTES;
        int index = 0;
        for(int rowIndexLength: rowIndexLengthList) {
            CodedInputStream codedInputStream = CodedInputStream.newInstance(bytes, currentOffset, rowIndexLength);
            try {
                rowGroupIndex[index++] = OrcProto.RowIndex.parseFrom(codedInputStream);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            currentOffset += rowIndexLength;
        }
        return rowGroupIndex;
    }

    public static int byteArrayToInt(byte[] b, int offset) {
        return   b[offset + 3] & 0xFF |
                (b[offset + 2] & 0xFF) << 8 |
                (b[offset + 1] & 0xFF) << 16 |
                (b[offset] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }


}
