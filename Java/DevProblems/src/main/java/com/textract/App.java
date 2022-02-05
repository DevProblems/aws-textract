package com.textract;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
* @author: Dev Problems
*/
public class App {

    public static Map<String, String> getRelationships(Map<String, Block> blockMap, Map<String, Block> keyMap,Map<String, Block> valueMap) {
        Map<String, String> result = new LinkedHashMap<>();
        for(Map.Entry<String, Block> itr : keyMap.entrySet()) {
            Block valueBlock = findValue(itr.getValue(), valueMap);
            String key = getText(itr.getValue(), blockMap);
            String value = getText(valueBlock, blockMap);
            result.put(key, value);
        }
        return result;
    }

    public static Block findValue(Block keyBlock, Map<String, Block> valueMap) {
        Block b = null;
        for(Relationship relationship : keyBlock.relationships()) {
            if(relationship.type().toString().equals("VALUE")) {
                for(String id : relationship.ids()) {
                    b = valueMap.get(id);
                }
            }
        }
        return b;
    }

    public static String getText(Block result, Map<String, Block> blockMap) {
        StringBuilder stringBuilder = new StringBuilder();
        for(Relationship relationship : result.relationships()) {
            if(relationship.type().toString().equals("CHILD")) {
                for(String id : relationship.ids()) {
                    Block b = blockMap.get(id);
                    if(b.blockTypeAsString().equals("WORD")) {
                        stringBuilder.append(b.text()).append(" ");
                    }
                 }
            }
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3client.getObject("s3bucketname", "filename");
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();

        SdkBytes bytes = SdkBytes.fromInputStream(s3ObjectInputStream);
        Document doc = Document.builder().bytes(bytes).build();

        List<FeatureType> list = new ArrayList<>();
        list.add(FeatureType.FORMS);

        AnalyzeDocumentRequest request = AnalyzeDocumentRequest.builder().featureTypes(list).document(doc).build();

        TextractClient textractClient = TextractClient.builder().region(Region.US_WEST_2).build();

        AnalyzeDocumentResponse response = textractClient.analyzeDocument(request);
        List<Block> blocks = response.blocks();

        Map<String, Block> blockMap = new LinkedHashMap<>();
        Map<String, Block> keyMap = new LinkedHashMap<>();
        Map<String, Block> valueMap = new LinkedHashMap<>();

        for (Block b : blocks) {
            String block_id = b.id();
            blockMap.put(block_id, b);
            if(b.blockTypeAsString().equals("KEY_VALUE_SET")) {
                for(EntityType entityType : b.entityTypes()) {
                    if(entityType.toString().equals("KEY")) {
                        keyMap.put(block_id, b);
                    } else {
                        valueMap.put(block_id, b);
                    }
                }
            }
        }
            System.out.println(getRelationships(blockMap, keyMap, valueMap));
            textractClient.close();

    }
}
